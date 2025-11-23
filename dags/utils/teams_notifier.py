import json
import requests
from airflow.models import Variable

def notify_teams(context, status: str):
    """
    Send a simple card message to Microsoft Teams with DAG status.
    status: "SUCCESS" or "FAILED"
    """
    webhook_url = Variable.get("TEAMS_WEBHOOK_URL")

    dag_run = context.get("dag_run")
    task_instance = context.get("ti")

    dag_id = dag_run.dag_id if dag_run else context['dag'].dag_id
    run_id = dag_run.run_id if dag_run else context.get("run_id")
    task_id = task_instance.task_id if task_instance else None
    execution_date = context.get("logical_date")

    if status == "SUCCESS":
        theme_color = "00FF00"  # green
        title = f"✅ Airflow DAG Succeeded: {dag_id}"
    else:
        theme_color = "FF0000"  # red
        title = f"❌ Airflow DAG Failed: {dag_id}"

    message = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "summary": f"Airflow DAG {status}: {dag_id}",
        "themeColor": theme_color,
        "title": title,
        "sections": [
            {
                "facts": [
                    {"name": "DAG ID", "value": dag_id},
                    {"name": "Run ID", "value": str(run_id)},
                    {"name": "Task ID", "value": str(task_id)},
                    {"name": "Execution Date", "value": str(execution_date)},
                    {"name": "Status", "value": status},
                ],
                "markdown": True,
            }
        ],
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(webhook_url, headers=headers, data=json.dumps(message))
    response.raise_for_status()
