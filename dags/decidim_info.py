from __future__ import annotations

import json
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

from data_utils.grist.decidim_info import K8sDecidimConfig, collect_and_push_to_grist

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="decidim_list_to_grist",
    description="Fetch Decidim CRs from Kubernetes and push them into a Grist document",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["decidim", "k8s", "grist"],
) as dag:
    @task
    def run_collection_and_push():
        cfg = K8sDecidimConfig(
            kube_conn_id="k8s_config",
            grist_conn_id="grist_osp",
            grist_doc_var="grist_decidim_document_id",
            grist_table_var="grist_osp_plateformes_decidim",
            api_version="apps.libre.sh/v1alpha1",
            kind="Decidim",
        )

        df = collect_and_push_to_grist(cfg)

        if df is None or df.empty:
            logging.info("No Decidim resources found.")
        else:
            logging.info("Found %d Decidim resources.", len(df))
            logging.debug(
                "Sample rows: %s",
                json.dumps(df.head(5).to_dict(orient="records"), ensure_ascii=False),
            )

    run_collection_and_push()
