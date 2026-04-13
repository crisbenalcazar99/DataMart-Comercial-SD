from datetime import datetime, timedelta
import logging

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from DataWarehouseFacturacion.common.session_manager import get_session
from DataWarehouseFacturacion.config.app_config import AppConfig
from DataWarehouseFacturacion.config.logger_config import setup_logger
from DataWarehouseFacturacion.utils.RunMode import RunMode
from DataWarehouseFacturacion.utils.db_utils import truncate_tables, create_all_tables

# Pipelines
from DataWarehouseFacturacion.pipelines.bronze.bronze_clientes_pipeline import BronzeClientesPipeline
from DataWarehouseFacturacion.pipelines.bronze.bronze_facturas_pipeline import BronzeFacturasPipeline
from DataWarehouseFacturacion.pipelines.bronze.bronze_tranfac_pipeline import BronzeTranfacPipeline
from DataWarehouseFacturacion.pipelines.bronze.bronze_vendedores_pipeline import BronzeVendedoresPipeline
from DataWarehouseFacturacion.pipelines.bronze.bronze_articulos_pipeline import BronzeArticulosPipeline
from DataWarehouseFacturacion.pipelines.bronze.bronze_codigos_pipeline import BronzeCodigosPipeline
from DataWarehouseFacturacion.pipelines.bronze.bronze_operatividad_pipeline import BronzeOperatividadPipeline

# Entities
from DataWarehouseFacturacion.entities.bronze.broze_facturas_entity import BronzeFacturaEntity
from DataWarehouseFacturacion.entities.bronze.bronze_clientes_entity import BronzeClienteEntity
from DataWarehouseFacturacion.entities.bronze.broze_vendedores_entity import BronzeVendedoresEntity
from DataWarehouseFacturacion.entities.bronze.broze_tranfac_entity import BronzeTranfacEntity
from DataWarehouseFacturacion.entities.bronze.bronze_articulos_entity import BronzeArticulosEntity
from DataWarehouseFacturacion.entities.bronze.bronze_codigos_entity import BronzeCodigosEntity
from DataWarehouseFacturacion.entities.bronze.bronze_operatividad_entity import BronzeOperatividadEntity

from cosmos.providers.dbt.task_group import DbtTaskGroup
from cosmos.config import ProjectConfig, ProfileConfig


# -------------------------
# CONFIG
# -------------------------
def get_config():
    return AppConfig(
        db_alias="LOCAL",
        run_mode=RunMode.INCREMENTAL
    )


# -------------------------
# TASK: INIT (FULL LOAD)
# -------------------------
def init_process(**kwargs):
    setup_logger()
    log = logging.getLogger(__name__)

    app_config = get_config()

    log.info("Inicio Proceso Datawarehouse Facturacion BY ZALY-CB")
    log.info(f"Modo: {app_config.run_mode} | DB: {app_config.db_alias}")


# -------------------------
# TASKS PIPELINES
# -------------------------
def run_operatividad():
    BronzeOperatividadPipeline(get_config()).run()


def run_codigos():
    BronzeCodigosPipeline(get_config()).run()


def run_articulos():
    BronzeArticulosPipeline(get_config()).run()


def run_facturas():
    BronzeFacturasPipeline(get_config()).run()


def run_tranfac():
    BronzeTranfacPipeline(get_config()).run()


def run_clientes():
    BronzeClientesPipeline(get_config()).run()


def run_vendedores():
    BronzeVendedoresPipeline(get_config()).run()



# -------------------------
# DAG
# -------------------------

default_args = {
    "owner": "airflow",
    "email": ["crisbenalp@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True
}

with DAG(
    dag_id="bronze_dwh_facturacion",
    start_date=datetime(2026, 1, 1),
    schedule="40 7,9,11,13,15,17 * * *",
    catchup=False,
    tags=["dwh", "fenix", "facturacion", "QUANTA", "FENIX"],
    default_args=default_args,
) as dag:

    init_task = PythonOperator(
        task_id="init_process",
        python_callable=init_process
    )

    operatividad_task = PythonOperator(
        task_id="bronze_operatividad",
        python_callable=run_operatividad
    )

    codigos_task = PythonOperator(
        task_id="bronze_codigos",
        python_callable=run_codigos
    )

    articulos_task = PythonOperator(
        task_id="bronze_articulos",
        python_callable=run_articulos
    )

    facturas_task = PythonOperator(
        task_id="bronze_facturas",
        python_callable=run_facturas
    )

    tranfac_task = PythonOperator(
        task_id="bronze_tranfac",
        python_callable=run_tranfac
    )

    clientes_task = PythonOperator(
        task_id="bronze_clientes",
        python_callable=run_clientes
    )

    vendedores_task = PythonOperator(
        task_id="bronze_vendedores",
        python_callable=run_vendedores
    )

    dbt_tasks = DbtTaskGroup(
        group_id="dbt_run",
        project_config=ProjectConfig("/opt/airflow/DataWarehouseFacturacion/dwh_facturacion_dbt"),
        profile_config=ProfileConfig(
            profile_name="dwh_facturacion_dbt",
            target_name="dev",
            profiles_yml_filepath="/opt/airflow/DataWarehouseFacturacion/config/dbt/profiles.yml"
        )
    )

    # -------------------------
    # DEPENDENCIAS
    # -------------------------

    init_task >> operatividad_task >> codigos_task >> articulos_task >> facturas_task >> tranfac_task >> clientes_task >> vendedores_task >> dbt_tasks