from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.etl.pipeline import (
    bootstrap_source_database,
    clean_datasets_task,
    export_csv_to_local_drive_task,
    extract_grammy_task,
    extract_spotify_task,
    load_data_warehouse_task,
    transform_and_merge_task,
)


with DAG(
    dag_id="spotify_grammy_etl",
    description="Workshop 2 ETL pipeline using Apache Airflow",
    start_date=datetime(2026, 4, 10),
    schedule=None,
    catchup=False,
    tags=["etl", "spotify", "grammy", "workshop"],
) as dag:
    bootstrap_grammy_db = PythonOperator(
        task_id="bootstrap_grammy_source_db",
        python_callable=bootstrap_source_database,
    )

    extract_spotify_csv = PythonOperator(
        task_id="extract_spotify_csv",
        python_callable=extract_spotify_task,
    )

    extract_grammy_db = PythonOperator(
        task_id="extract_grammy_db",
        python_callable=extract_grammy_task,
    )

    clean_datasets = PythonOperator(
        task_id="clean_datasets",
        python_callable=clean_datasets_task,
    )

    transform_and_merge = PythonOperator(
        task_id="transform_and_merge",
        python_callable=transform_and_merge_task,
    )

    export_csv_to_local_drive = PythonOperator(
        task_id="export_csv_to_local_drive",
        python_callable=export_csv_to_local_drive_task,
    )

    load_data_warehouse = PythonOperator(
        task_id="load_data_warehouse",
        python_callable=load_data_warehouse_task,
    )

    bootstrap_grammy_db >> extract_grammy_db
    extract_spotify_csv >> clean_datasets
    extract_grammy_db >> clean_datasets
    clean_datasets >> transform_and_merge >> export_csv_to_local_drive >> load_data_warehouse

