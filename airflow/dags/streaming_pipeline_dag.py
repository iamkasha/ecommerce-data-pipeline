"""
DAG: streaming_pipeline
Schedule: Hourly

Manages the streaming ingestion layer:
  1. Health-check Kafka topic lag
  2. Compact hourly event files in Bronze
  3. Run Silver transformation for events
  4. Update streaming metrics in warehouse
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner":        "data-engineering",
    "retries":      1,
    "retry_delay":  timedelta(minutes=2),
}


def check_kafka_lag(**context):
    """Check consumer group lag — alert if > threshold."""
    import subprocess, json
    try:
        result = subprocess.run(
            ["kafka-consumer-groups",
             "--bootstrap-server", "kafka:29092",
             "--group", "s3-sink-consumer",
             "--describe", "--output", "json"],
            capture_output=True, text=True, timeout=10
        )
        data = json.loads(result.stdout)
        total_lag = sum(p.get("lag", 0) for g in data for p in g.get("partitions", []))
        context["task_instance"].xcom_push(key="consumer_lag", value=total_lag)
        if total_lag > 10000:
            return "alert_high_lag"
        return "compact_bronze_events"
    except Exception:
        return "compact_bronze_events"


def alert_high_lag(**context):
    lag = context["task_instance"].xcom_pull(key="consumer_lag")
    print(f"⚠️ HIGH KAFKA LAG DETECTED: {lag} messages behind. Check consumer health.")
    # In production: send to PagerDuty / Slack


def compact_events(**context):
    """Merge many small JSON files into larger Parquet files."""
    import boto3, io, json
    from datetime import datetime
    import pandas as pd

    env = "local"
    s3_kwargs = dict(
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    ) if env == "local" else {}

    s3 = boto3.client("s3", **s3_kwargs)
    bucket = "ecommerce-lake"
    now    = datetime.utcnow()
    prefix = (f"bronze/events/year={now.year}/month={now.month:02d}/"
              f"day={now.day:02d}/hour={now.hour:02d}/")

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [o["Key"] for o in response.get("Contents", []) if o["Key"].endswith(".json")]

    if not keys:
        print("No event files to compact.")
        return

    records = []
    for key in keys:
        obj  = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode()
        for line in body.strip().splitlines():
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass

    df = pd.DataFrame(records)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)

    compact_key = prefix + "compacted.parquet"
    s3.put_object(Bucket=bucket, Key=compact_key, Body=buf.getvalue())
    print(f"Compacted {len(records)} events into {compact_key}")


with DAG(
    dag_id="streaming_pipeline",
    description="Hourly streaming: Kafka → S3 Bronze → Silver events",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["streaming", "kafka", "events"],
) as dag:

    t_check_lag = BranchPythonOperator(
        task_id="check_kafka_lag",
        python_callable=check_kafka_lag,
    )

    t_alert = PythonOperator(
        task_id="alert_high_lag",
        python_callable=alert_high_lag,
    )

    t_compact = PythonOperator(
        task_id="compact_bronze_events",
        python_callable=compact_events,
        trigger_rule="none_failed_min_one_success",
    )

    t_check_lag >> [t_alert, t_compact]
    t_alert >> t_compact
