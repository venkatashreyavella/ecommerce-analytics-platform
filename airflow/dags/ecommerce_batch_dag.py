"""Daily batch ingestion and dbt transformation DAG."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

LOGGER = logging.getLogger(__name__)
RAW_DATA_DIR = Path("/raw_data")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def find_new_batch_files(**context: Any) -> list[str]:
    files = sorted(str(path) for path in RAW_DATA_DIR.glob("*.jsonl") if path.is_file())
    if not files:
        raise AirflowSkipException(f"No JSONL files found in {RAW_DATA_DIR}")
    LOGGER.info("Found %s landing files", len(files))
    context["ti"].xcom_push(key="batch_files", value=files)
    return files


def database_connection():
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "postgres"),
        port=os.getenv("WAREHOUSE_PORT", "5432"),
        dbname=os.getenv("WAREHOUSE_DB", "warehouse"),
        user=os.getenv("WAREHOUSE_USER", "warehouse"),
        password=os.getenv("WAREHOUSE_PASSWORD", "warehouse"),
        connect_timeout=10,
    )


def load_batch_files(**context: Any) -> None:
    files = context["ti"].xcom_pull(task_ids="check_new_batch_data", key="batch_files") or []
    if not files:
        raise AirflowSkipException("No files were returned by the landing check")

    connection = database_connection()
    try:
        with connection:
            with connection.cursor() as cursor:
                for filename in files:
                    file_path = Path(filename)
                    if file_path.name.startswith("users_"):
                        table = "raw.raw_users"
                        columns = ("user_id", "email", "first_name", "last_name", "country", "signup_date")
                    elif file_path.name.startswith("products_"):
                        table = "raw.raw_products"
                        columns = ("product_id", "product_name", "category", "price", "updated_at")
                    elif file_path.name.startswith("orders_"):
                        table = "raw.raw_orders"
                        columns = ("order_id", "user_id", "product_id", "session_id", "quantity", "unit_price", "order_timestamp")
                    else:
                        LOGGER.warning("Ignoring unrecognized landing file %s", filename)
                        continue

                    placeholders = ", ".join(["%s"] * len(columns))
                    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
                    inserted = 0
                    with file_path.open(encoding="utf-8") as source:
                        for line in source:
                            if not line.strip():
                                continue
                            record = json.loads(line)
                            cursor.execute(query, tuple(record[column] for column in columns))
                            inserted += cursor.rowcount
                    LOGGER.info("Loaded %s new rows from %s", inserted, filename)
    finally:
        connection.close()


with DAG(
    dag_id="ecommerce_batch_daily",
    default_args=DEFAULT_ARGS,
    description="Load generated landing files and rebuild the ecommerce dbt star schema",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "batch", "dbt"],
) as dag:
    check_new_batch_data = PythonOperator(
        task_id="check_new_batch_data",
        python_callable=find_new_batch_files,
    )

    load_raw_data = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_batch_files,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir /opt/airflow/dbt_project",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project && dbt test --profiles-dir /opt/airflow/dbt_project",
    )

    check_new_batch_data >> load_raw_data >> dbt_run >> dbt_test
