from sqlalchemy import inspect


from datawarehouse.common.session_manager import get_session
from datawarehouse.models.base import Base
# from models.Comercial.clientes_entity import ClientesEntity
# from models.Comercial.facturas_entity import FacturasEntity
# from models.Comercial.transacciones_entity import TransaccionesEntity
# from models.Comercial.vendedores_entity import VendedoresEntity
# from models.Comercial.reasignaciones_entity import ReasignacionesEntity
# from models.catalogos import CatalogosEntity
# from models.Comercial.articulos_entity import ArticulosEntity
# from pipelines.comercial.fact_table import FactTable


from datawarehouse.pipelines.Zoho.dim_usuarios_pipeline import RucsPipeline
from datawarehouse.pipelines.Zoho.dim_rucs_pipeline import UsersPipeline
from datawarehouse.pipelines.Zoho.fact_servicios_activos_pipeline import CurrentProductsPipeline
from datawarehouse.pipelines.Zoho.fact_transacciones_operatividad_pipeline import TransactionHistoryPipeline
from datawarehouse.pipelines.Zoho.staging_integrador_pipeline import StagingIntegradorOperatividad
from datawarehouse.utils.RunMode import RunMode

# from pipelines.comercial import dim_clientes
# from pipelines.comercial import dim_vendedores
# from pipelines.comercial import dim_reasignaciones
# from pipelines.comercial.fact_table import FactTable
from datawarehouse.config.logger_config import setup_logger
import logging


def create_all_tables():
    with get_session("QUANTA") as session:
        Base.metadata.create_all(session.bind)
        inspector = inspect(session.bind)
        print(inspector.get_table_names())


#create_all_tables()
#dim_clientes.ejecutar_pipeline()
#FactTable.load_transaccciones_facturas()

if __name__ == "__main__":
    setup_logger()
    log = logging.getLogger(__name__)
    log.info("Inicio Proceso Datamart Comercial BY ZALY-CB")

    #FactTable.load_transaccciones_facturas()
    # dim_users.ejecutar_pipeline(df_general)

    df_general = StagingIntegradorOperatividad.run(RunMode.INCREMENTAL)
    UsersPipeline().run(df_general) # Pipeline carga de Usuarios
    RucsPipeline().run(df_general) # Pipeline Proceso de Carga de RUCS
    df_transacciones_operatividad = TransactionHistoryPipeline(RunMode.INCREMENTAL).run(df_general)
    CurrentProductsPipeline().run(df_transacciones_operatividad)


