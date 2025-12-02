from sqlalchemy import inspect
from datawarehouse.common.session_manager import get_session
from datawarehouse.models.base import Base

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


from datawarehouse.config.logger_config import setup_logger
import logging

from datawarehouse.pipelines.comercial.dim_clientes import ClientesPipeline
from datawarehouse.pipelines.comercial.dim_facturas_pipeline import FacturasComercialPipeline
from datawarehouse.pipelines.comercial.fact_transacciones_pipeline import TransactionDetailsPipeline
from datawarehouse.pipelines.comercial.staging_integrador_pipeline import StagingIntegradorComercial


def create_all_tables():
    with get_session("QUANTA") as session:
        Base.metadata.create_all(session.bind)
        inspector = inspect(session.bind)
        print(inspector.get_table_names())


create_all_tables()

if __name__ == "__main__":
    setup_logger()
    log = logging.getLogger(__name__)
    log.info("Inicio Proceso Datamart Comercial BY ZALY-CB")

    # ---------------- COMERCIAL ------------------------
    df_general = StagingIntegradorComercial.run()
    print(df_general.info())
    ClientesPipeline().run(df_general)
    FacturasComercialPipeline().run(df_general)
    TransactionDetailsPipeline().run(df_general)





    # ---------------- OPERATIVIDAD ----------------------------------
    # df_general = StagingIntegradorOperatividad.run(RunMode.INCREMENTAL)
    # UsersPipeline().run(df_general) # Pipeline carga de Usuarios
    # RucsPipeline().run(df_general) # Pipeline Proceso de Carga de RUCS
    # df_transacciones_operatividad = TransactionHistoryPipeline(RunMode.INCREMENTAL).run(df_general)
    # CurrentProductsPipeline().run(df_transacciones_operatividad)
    # ---------------- OPERATIVIDAD ----------------------------------


