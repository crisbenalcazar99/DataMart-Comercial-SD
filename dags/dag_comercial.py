from datetime import timedelta, datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from datawarehouse.models.zoho.dim_localidades import DimLocalidadesEntity
from datawarehouse.models.zoho.dim_rucs import DimRucsEntity
from datawarehouse.models.zoho.dim_usuarios import DimUsuariosEntity
from datawarehouse.models.zoho.fact_transacciones_operatividad import FactTransaccionesOperatividadEntity
from datawarehouse.models.zoho.fact_servicios_activos import FactServiciosActivosEntity

from datawarehouse.models.catalogos import CatalogosEntity
from datawarehouse.models.Comercial.dim_clientes_entity import DimClientesEntity
from datawarehouse.models.Comercial.dim_articulos_entity import DimArticulosEntity
from datawarehouse.models.Comercial.dim_vendedores_entity import DimVendedoresEntity
from datawarehouse.models.Comercial.fact_facturas_entity import FactFacturasEntity
from datawarehouse.models.Comercial.fact_detalle_transacciones_entity import FactDetalleTransaccionesEntity

from datawarehouse.pipelines.comercial.dim_clientes import ClientesPipeline
from datawarehouse.pipelines.comercial.dim_facturas_pipeline import FacturasComercialPipeline
from datawarehouse.pipelines.comercial.fact_transacciones_pipeline import TransactionDetailsPipeline
from datawarehouse.pipelines.comercial.staging_integrador_pipeline import StagingIntegradorComercial

import logging

from pendulum import timezone
local_tz = timezone("America/Guayaquil")
log = logging.getLogger(__name__)


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
        dag_id="dag_comercial_zoho",
        description="DAG Comercial ZOHO",
        start_date=datetime(2026, 1, 28, tzinfo=local_tz),
        schedule="25 7,10,13,16 * * *",
        catchup=False,
        tags=["comercial", "ZOHO", "QUANTA", "FENIX"],
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
