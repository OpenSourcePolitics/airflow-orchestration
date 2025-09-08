from __future__ import annotations
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

PARIS = pendulum.timezone("Europe/Paris")

PG_CONN_ID = "ai_logs_pg"

BASE_DIR = Path(__file__).resolve().parent
SQL_PATH = BASE_DIR / "data_utils" / "ai_aggregation"

with DAG(
    dag_id="pg_ai_observation_maintenance_daily",
    description="Create partitions, apply retention, upsert daily aggregates",
    start_date=pendulum.datetime(2025, 9, 1, tz=PARIS),
    schedule="10 2 * * *",
    catchup=False,
    tags=["postgres", "maintenance"],
    template_searchpath=[str(SQL_PATH)],
) as dag:

    premake = SQLExecuteQueryOperator(
        task_id="premake_partitions",
        conn_id=PG_CONN_ID,
        sql="premake_partitions.sql",
        split_statements=True,
    )

    retention = SQLExecuteQueryOperator(
        task_id="drop_old_partitions",
        conn_id=PG_CONN_ID,
        sql="drop_old_partitions.sql",
        split_statements=True,
    )

    upsert_daily = SQLExecuteQueryOperator(
        task_id="upsert_daily_yesterday",
        conn_id=PG_CONN_ID,
        sql="upsert_daily.sql",
        split_statements=True,
        parameters={
            "target_day": "{{ macros.ds_add(ds, -1) }}",  # -> 'YYYY-MM-DD'
        },
    )

    premake >> retention >> upsert_daily
