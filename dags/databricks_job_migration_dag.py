from utils.teams_notifier import notify_teams
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import XCOM_RUN_ID_KEY, XCOM_RUN_PAGE_URL_KEY

default_args = {
    "owner": "data_eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def store_run_info_to_postgres(**context):
    """
    1. Get run_id from XCom (pushed by DatabricksRunNowOperator)
    2. Call Databricks Jobs API to get run details
    3. Insert a row into Postgres table databricks_job_runs
    """
    ti = context["ti"]
    dag = context["dag"]

    # 1) Get run_id and run_page_url from XCom
    run_id = ti.xcom_pull(
        task_ids="every_min_job_weatherAPI",
        key=XCOM_RUN_ID_KEY,     # usually "run_id"
    )
    run_page_url = ti.xcom_pull(
        task_ids="every_min_job_weatherAPI",
        key=XCOM_RUN_PAGE_URL_KEY,   # usually "run_page_url"
    )

    if run_id is None:
        raise ValueError(
            "No Databricks run_id in XCom from task 'every_min_job_weatherAPI'. "
            "Check that do_xcom_push=True on DatabricksRunNowOperator and that task succeeded."
        )

    # 2) Get run details from Databricks
    dbx_hook = DatabricksHook(databricks_conn_id="databricks_default")
    run = dbx_hook.get_run(run_id=run_id)

    job_id = run.get("job_id")
    state = run.get("state") or {}
    life_cycle_state = state.get("life_cycle_state")
    result_state = state.get("result_state")
    state_message = state.get("state_message")

    from datetime import datetime, timezone

    def ms_to_dt(ms):
        if ms is None:
            return None
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)

    start_time = ms_to_dt(run.get("start_time"))
    end_time = ms_to_dt(run.get("end_time"))

    # 2.5) Calculate duration
    if start_time and end_time:
        duration_seconds = (end_time - start_time).total_seconds()
    else:
        duration_seconds = None  # still running or failed early

    # Airflow metadata
    execution_date = context["logical_date"]
    airflow_run_id = context["run_id"]
    airflow_dag_id = dag.dag_id
    airflow_task_id = "every_min_job_weatherAPI"

    # 3) Insert into Postgres
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO databricks_job_runs (
            airflow_dag_id,
            airflow_task_id,
            airflow_run_id,
            execution_date,
            job_id,
            databricks_run_id,
            life_cycle_state,
            result_state,
            state_message,
            start_time,
            end_time,
            duration_seconds,
            run_page_url
        )
        VALUES (%(airflow_dag_id)s,
                %(airflow_task_id)s,
                %(airflow_run_id)s,
                %(execution_date)s,
                %(job_id)s,
                %(databricks_run_id)s,
                %(life_cycle_state)s,
                %(result_state)s,
                %(state_message)s,
                %(start_time)s,
                %(end_time)s,
                %(duration_seconds)s,
                %(run_page_url)s);
    """

    params = {
        "airflow_dag_id": airflow_dag_id,
        "airflow_task_id": airflow_task_id,
        "airflow_run_id": airflow_run_id,
        "execution_date": execution_date,
        "job_id": job_id,
        "databricks_run_id": run_id,
        "life_cycle_state": life_cycle_state,
        "result_state": result_state,
        "state_message": state_message,
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": duration_seconds,
        "run_page_url": run_page_url,
    }

    cursor.execute(insert_sql, params)
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id="databricks_job_migrated",
    default_args=default_args,
    schedule="*/5 * * * *",  # every 5 minute
    start_date=datetime(2025, 11, 21),
    catchup=False,
    on_success_callback=lambda context: notify_teams(context, "SUCCESS"),
    on_failure_callback=lambda context: notify_teams(context, "FAILED"),
) as dag:

    run_databricks_job = DatabricksRunNowOperator(
        task_id="every_min_job_weatherAPI",
        databricks_conn_id="databricks_default",
        job_id=36559872929425,
        notebook_params={
            "run_date": "{{ ds }}",
            "env": "prod",
        },
        # IMPORTANT: enable XCom so we get run_id
        do_xcom_push=True,
    )

    save_run_monitoring = PythonOperator(
        task_id="save_run_monitoring_to_postgres",
        python_callable=store_run_info_to_postgres,
        op_kwargs={"dag": dag},
    )

    run_databricks_job >> save_run_monitoring
