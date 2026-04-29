"""
DAG: batch_ingestion
Schedule: Daily @ 02:00 UTC

Full batch pipeline:
  1. Extract source PostgreSQL → Bronze S3
  2. PySpark: Bronze → Silver (clean + validate)
  3. Data Quality checks (Great Expectations)
  4. PySpark: Silver → Gold (aggregates)
  5. Load Gold → Warehouse (Redshift / PostgreSQL)
"""
import sys

sys.path.insert(0, "/opt/airflow")

from datetime import timedelta  # noqa: E402

from airflow import DAG  # noqa: E402
from airflow.operators.bash import BashOperator  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402
from airflow.utils.dates import days_ago  # noqa: E402
from data_quality.validate import run_validation  # noqa: E402
from ingestion.batch.postgres_extractor import run_batch  # noqa: E402
from processing.bronze_to_silver.clean_orders import run as clean_orders  # noqa: E402
from processing.bronze_to_silver.clean_users import run_products, run_users  # noqa: E402
from processing.silver_to_gold.aggregate_daily import run as aggregate  # noqa: E402
from warehouse.loaders.redshift_loader import run as load_warehouse  # noqa: E402

# ── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


def extract(**context):
    batch_date = context["ds_nodash"]
    from datetime import date
    d = date(int(batch_date[:4]), int(batch_date[4:6]), int(batch_date[6:]))
    run_batch(d)


def transform_orders(**context):
    from datetime import date
    d = date.fromisoformat(context["ds"])
    clean_orders(d)


def transform_users(**context):
    from datetime import date
    d = date.fromisoformat(context["ds"])
    run_users(d)
    run_products(d)


def quality_check(**context):
    from datetime import date
    d = date.fromisoformat(context["ds"])
    run_validation(d)


def aggregate_gold(**context):
    aggregate()


def load_wh(**context):
    from datetime import date
    d = date.fromisoformat(context["ds"])
    load_warehouse(batch_date=d)


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="batch_ingestion",
    description="Daily batch pipeline: Postgres → S3 → Spark → Warehouse",
    schedule_interval="0 2 * * *",
    start_date=days_ago(7),
    catchup=False,
    default_args=default_args,
    tags=["batch", "ingestion", "etl"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_postgres_to_bronze",
        python_callable=extract,
    )

    t_clean_orders = PythonOperator(
        task_id="bronze_to_silver_orders",
        python_callable=transform_orders,
    )

    t_clean_users = PythonOperator(
        task_id="bronze_to_silver_users_products",
        python_callable=transform_users,
    )

    t_quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=quality_check,
    )

    t_aggregate = PythonOperator(
        task_id="silver_to_gold_aggregates",
        python_callable=aggregate_gold,
    )

    t_load_wh = PythonOperator(
        task_id="load_gold_to_warehouse",
        python_callable=load_wh,
    )

    t_dbt = BashOperator(
        task_id="dbt_transformations",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --target dev",
    )

    t_dbt_test = BashOperator(
        task_id="dbt_tests",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir . --target dev",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    t_extract >> [t_clean_orders, t_clean_users]
    [t_clean_orders, t_clean_users] >> t_quality
    t_quality >> t_aggregate
    t_aggregate >> t_load_wh
    t_load_wh >> t_dbt
    t_dbt >> t_dbt_test
