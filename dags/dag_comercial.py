from datetime import timedelta, datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from datawarehouse.pipelines.comercial.dim_clientes import ClientesPipeline
from datawarehouse.pipelines.comercial.dim_facturas_pipeline import FacturasComercialPipeline
from datawarehouse.pipelines.comercial.fact_transacciones_pipeline import TransactionDetailsPipeline
from datawarehouse.pipelines.comercial.staging_integrador_pipeline import StagingIntegradorComercial


def run_staging(**context):
    df_general = StagingIntegradorComercial.run()
    ti = context["ti"]
    ti.xcom_push(key="df_general", value=df_general)


def run_clients(**context):
    ti = context["ti"]
    df_general = ti.xcom_pull(
        task_ids="staging_integrador",
        key="df_general"
    )
    ClientesPipeline().run(df_general)


def run_facturas(**context):
    ti = context["ti"]
    df_general = ti.xcom_pull(
        task_ids="staging_integrador",
        key="df_general"
    )
    FacturasComercialPipeline().run(df_general)


def run_transaction(**context):
    ti = context["ti"]
    df_general = ti.xcom_pull(
        task_ids="staging_integrador",
        key="df_general"
    )
    TransactionDetailsPipeline().run(df_general)


default_args = {
    "owner": "airflow",
    "email": ["bi@securitydata.net.ec"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True
}

with DAG(
    dag_id = "dag_comercial_zoho",
    description="DAG Comercial ZOHO",
    start_date=datetime(2026, 1, 28),
    schedule="30 7,10,13,16 * * *",
    catchup=False,
    tags = ["comercial", "ZOHO", "QUANTA", "FENIX"],
    default_args=default_args,
) as dag:
    task_staging = PythonOperator(
        task_id="staging_integrador",
        python_callable=run_staging,
    )

    task_clients = PythonOperator(
        task_id="load_clients",
        python_callable=run_clients,
    )

    task_facturas = PythonOperator(
        task_id="load_facturas",
        python_callable=run_facturas
    )

    task_transactions = PythonOperator(
        task_id="load_transactions",
        python_callable=run_transaction,
    )

    task_staging >> task_clients >> task_facturas >> task_transactions
