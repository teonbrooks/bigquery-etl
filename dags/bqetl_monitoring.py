# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2018, 10, 30, 0, 0),
    "email": ["ascholtz@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_monitoring", default_args=default_args, schedule_interval="0 2 * * *"
) as dag:

    monitoring__bigquery_etl_scheduled_queries_cost__v1 = gke_command(
        task_id="monitoring__bigquery_etl_scheduled_queries_cost__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring/bigquery_etl_scheduled_queries_cost_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="mozilla/bigquery-etl:latest",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com"],
    )

    monitoring__bigquery_etl_scheduled_query_usage__v1 = gke_command(
        task_id="monitoring__bigquery_etl_scheduled_query_usage__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring/bigquery_etl_scheduled_query_usage_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="mozilla/bigquery-etl:latest",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com"],
    )

    monitoring__column_size__v1 = gke_command(
        task_id="monitoring__column_size__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring/column_size_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="mozilla/bigquery-etl:latest",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com"],
    )

    monitoring__stable_table_sizes__v1 = gke_command(
        task_id="monitoring__stable_table_sizes__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring/stable_table_sizes_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="mozilla/bigquery-etl:latest",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com"],
    )

    monitoring__structured_distinct_docids__v1 = gke_command(
        task_id="monitoring__structured_distinct_docids__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring/structured_distinct_docids_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="mozilla/bigquery-etl:latest",
        owner="bewu@mozilla.com",
        email=["ascholtz@mozilla.com", "bewu@mozilla.com"],
    )

    monitoring__telemetry_distinct_docids__v1 = bigquery_etl_query(
        task_id="monitoring__telemetry_distinct_docids__v1",
        destination_table="telemetry_distinct_docids_v1",
        dataset_id="monitoring",
        project_id="moz-fx-data-shared-prod",
        owner="bewu@mozilla.com",
        email=["ascholtz@mozilla.com", "bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    monitoring__column_size__v1.set_upstream(wait_for_copy_deduplicate_main_ping)

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    monitoring__stable_table_sizes__v1.set_upstream(wait_for_copy_deduplicate_all)
    monitoring__stable_table_sizes__v1.set_upstream(wait_for_copy_deduplicate_main_ping)

    monitoring__structured_distinct_docids__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    monitoring__structured_distinct_docids__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    monitoring__telemetry_distinct_docids__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    monitoring__telemetry_distinct_docids__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )