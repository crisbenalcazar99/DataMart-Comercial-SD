from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

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

from datawarehouse.pipelines.Zoho.actualizacion_emision_pipeline import UpdateTrasactionHistoryPipeline
from datawarehouse.pipelines.Zoho.dim_users_pipeline import UsersPipeline
from datawarehouse.pipelines.Zoho.dim_rucs_pipeline import RucsPipeline
from datawarehouse.pipelines.Zoho.fact_servicios_activos_pipeline import CurrentProductsPipeline
from datawarehouse.pipelines.Zoho.fact_transacciones_operatividad_pipeline import TransactionHistoryPipeline
from datawarehouse.pipelines.Zoho.staging_integrador_pipeline import StagingIntegradorOperatividad
from datawarehouse.utils.RunMode import RunMode

from pendulum import timezone

local_tz = timezone("America/Guayaquil")

log = logging.getLogger(__name__)


# -------------------------------------------
# FUNCIONES PARA CADA TAREA
# -------------------------------------------

def run_staging(**context):
    df_general = StagingIntegradorOperatividad.run(RunMode.INCREMENTAL)
    ti = context["ti"]
    ti.xcom_push(key="df_general", value=df_general)


def run_users(**context):
    ti = context["ti"]
    # 👇 OBLIGATORIO: indicar de qué tarea leer el XCom
    df_general = ti.xcom_pull(
        task_ids="staging_integrador",
        key="df_general",
    )
    UsersPipeline(run_mode=RunMode.INCREMENTAL).run(df_general)


def run_rucs(**context):
    ti = context["ti"]
    df_general = ti.xcom_pull(
        task_ids="staging_integrador",
        key="df_general",
    )
    RucsPipeline().run(df_general)


def run_transacciones(**context):
    ti = context["ti"]
    df_general = ti.xcom_pull(
        task_ids="staging_integrador",
        key="df_general",
    )
    df_transacciones_operatividad = TransactionHistoryPipeline(
        RunMode.INCREMENTAL
    ).run(df_general)
    ti.xcom_push(key="df_trans", value=df_transacciones_operatividad)


def run_current_products(**context):
    ti = context["ti"]
    df_transacciones_operatividad = ti.xcom_pull(
        task_ids="load_transacciones",
        key="df_trans",
    )
    CurrentProductsPipeline().run(df_transacciones_operatividad)


def run_update_history(**context):
    UpdateTrasactionHistoryPipeline().run()


default_args = {
    "owner": "airflow",
    "email": ["bi@securitydata.net.ec"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
}

with DAG(
        dag_id="dag_operatividad_incremental",
        description="DAG Actualizacion Incremental Informacion de Operatividad Zoho",
        start_date=datetime(2025, 11, 21, tzinfo=local_tz),
        schedule="30 7,10,13,16 * * *",
        catchup=False,
        tags=["operatividad", "ZOHO", "CAMUNDA", "QUANTA", "Incremental"],
        default_args=default_args,

) as dag:
    task_staging = PythonOperator(
        task_id="staging_integrador",
        python_callable=run_staging,
    )

    task_users = PythonOperator(
        task_id="load_users",
        python_callable=run_users,
    )

    task_rucs = PythonOperator(
        task_id="load_rucs",
        python_callable=run_rucs,
    )

    task_transacciones = PythonOperator(
        task_id="load_transacciones",
        python_callable=run_transacciones,
    )

    task_current_products = PythonOperator(
        task_id="load_current_products",
        python_callable=run_current_products,
    )

    task_run_update_history = PythonOperator(
        task_id="update_expiration_emision_date",
        python_callable=run_update_history,
    )

    task_staging >> task_users >> task_rucs >> task_transacciones >> task_current_products >> task_run_update_history
