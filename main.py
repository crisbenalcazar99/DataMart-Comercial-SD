from common.session_manager import get_session
from models.Comercial.clientes_entity import ClientesEntity
from models.Comercial.facturas_entity import FacturasEntity
from models.Comercial.transacciones_entity import TransaccionesEntity
from models.Comercial.vendedores_entity import VendedoresEntity
from models.Comercial.reasignaciones_entity import ReasignacionesEntity
from models.catalogos import CatalogosEntity
from models.Comercial.articulos_entity import ArticulosEntity
from models.base import Base
from pipelines.comercial.fact_table import FactTable

from pipelines.comercial import dim_clientes
from pipelines.comercial import dim_vendedores
from pipelines.comercial import dim_reasignaciones
from utils.general_functions import get_proyect_root, load_sql_statement
from config.logger_config import setup_logger
import logging


def create_all_tables():
    with get_session("LOCAL") as session:
        Base.metadata.create_all(session.bind)


#create_all_tables()
#dim_clientes.ejecutar_pipeline()
#FactTable.load_transaccciones_facturas()

if __name__ == "__main__":
    setup_logger()
    log = logging.getLogger(__name__)
    log.info("Inicio Proceso Datamart Comercial BY ZALY-CB")
    FactTable.load_transaccciones_facturas()
