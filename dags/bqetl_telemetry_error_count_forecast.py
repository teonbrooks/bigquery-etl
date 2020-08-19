# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query

default_args = {
    "owner": "bewu@mozilla.com",
    "start_date": datetime.datetime(2020, 8, 20, 0, 0),
    "email": ["bewu@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1200),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_telemetry_error_count_forecast",
    default_args=default_args,
    schedule_interval="0 2 * * *",
) as dag:

    monitoring__telemetry_parse_payload_error_forecast__v1 = bigquery_etl_query(
        task_id="monitoring__telemetry_parse_payload_error_forecast__v1",
        destination_table="telemetry_parse_payload_error_forecast_v1",
        dataset_id="monitoring",
        project_id="moz-fx-data-shared-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    monitoring__telemetry_error_counts__v1 = bigquery_etl_query(
        task_id="monitoring__telemetry_error_counts__v1",
        destination_table="telemetry_error_counts_v1",
        dataset_id="monitoring",
        project_id="moz-fx-data-shared-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    monitoring__telemetry_error_counts__v1.set_upstream(wait_for_copy_deduplicate_all)
