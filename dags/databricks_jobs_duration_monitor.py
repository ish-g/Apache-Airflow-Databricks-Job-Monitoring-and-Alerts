from datetime import datetime, timedelta
import json
import requests

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable

# Per-job duration thresholds in seconds
# 🔧 Adjust as needed (you can add more job_ids or change defaults)
JOB_DURATION_THRESHOLDS = {
    36559872929425: 25,   # e.g. your weatherAPI job should run at least 60 seconds
    # 12345678901234: 300,  # another job: 5 minutes
}
DEFAULT_THRESHOLD_SECONDS = 30  # fallback if job_id not in dict


def send_teams_alert(short_runs):
    """
    Send a single Teams alert listing all jobs whose last run duration
    is below configured threshold.
    """
    if not short_runs:
        return

    webhook_url = Variable.get("TEAMS_WEBHOOK_URL")

    # Build a Markdown list of suspicious jobs
    lines = []
    for item in short_runs:
        job_id = item["job_id"]
        run_id = item["run_id"]
        duration = item["duration_seconds"]
        threshold = item["threshold"]
        run_page_url = item["run_page_url"]
        start_time = item["start_time"]

        lines.append(
            f"- **Job ID:** {job_id} | **Run ID:** {run_id} | "
            f"**Duration:** {duration:.1f}s (threshold: {threshold}s) | "
            f"**Start:** {start_time} | [Run page]({run_page_url})"
        )

    text_body = "\n".join(lines)

    card = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "summary": "Databricks job duration alerts",
        "themeColor": "FFA500",  # orange
        "title": "⚠️ Databricks jobs with suspiciously short durations",
        "sections": [
            {
                "activityTitle": "Please check if these jobs skipped work or failed silently.",
                "markdown": True,
                "text": text_body,
            }
        ],
    }

    headers = {"Content-Type": "application/json"}
    resp = requests.post(webhook_url, headers=headers, data=json.dumps(card))
    resp.raise_for_status()


def check_latest_job_runs(**context):
    """
    For each job_id in databricks_job_runs:
      - find the latest run
      - compare duration_seconds with configured threshold
      - if less than threshold -> collect and send Teams alert
    """
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    query = """
        WITH ranked_runs AS (
            SELECT
                job_id,
                databricks_run_id,
                duration_seconds,
                run_page_url,
                life_cycle_state,
                result_state,
                start_time,
                end_time,
                ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY start_time DESC) AS rn
            FROM databricks_job_runs
        )
        SELECT
            job_id,
            databricks_run_id,
            duration_seconds,
            run_page_url,
            life_cycle_state,
            result_state,
            start_time,
            end_time
        FROM ranked_runs
        WHERE rn = 1;
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    short_runs = []

    for row in rows:
        job_id = row[0]
        run_id = row[1]
        duration_seconds = row[2]
        run_page_url = row[3]
        life_cycle_state = row[4]
        result_state = row[5]
        start_time = row[6]
        end_time = row[7]

        # Skip if no duration yet (run still running or no times)
        if duration_seconds is None:
            continue

        # Determine threshold for this job
        threshold = JOB_DURATION_THRESHOLDS.get(job_id, DEFAULT_THRESHOLD_SECONDS)

        # Optional: only consider finished runs
        if life_cycle_state not in ("TERMINATED", "INTERNAL_ERROR"):
            continue

        # If duration is less than threshold -> suspicious
        if duration_seconds < threshold:
            short_runs.append(
                {
                    "job_id": job_id,
                    "run_id": run_id,
                    "duration_seconds": float(duration_seconds),
                    "threshold": threshold,
                    "run_page_url": run_page_url,
                    "start_time": start_time,
                    "result_state": result_state,
                    "life_cycle_state": life_cycle_state,
                }
            )

    # Send a single Teams alert listing all suspicious jobs
    if short_runs:
        send_teams_alert(short_runs)


# ───────────────────────── DAG DEFINITION ─────────────────────────

default_args = {
    "owner": "data_eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="databricks_jobs_duration_monitor",
    default_args=default_args,
    schedule="* * * * *",  # every 5 minutes
    start_date=datetime(2025, 11, 23),
    catchup=False,
    description="Monitor Databricks job durations and alert on unusually short runs",
) as dag:

    check_and_alert = PythonOperator(
        task_id="check_latest_runs_and_alert",
        python_callable=check_latest_job_runs,
    )
