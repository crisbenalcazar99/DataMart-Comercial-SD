from datawarehouse.config import LoggingPipeline
from datawarehouse.etl.extract import DatabaseExtractor
from datawarehouse.etl.load.db_load_test import DWLoader
from datawarehouse.etl.transform import IdentifyChangeNewClients
from datawarehouse.etl.transform import DtypeIntegerTransform, DtypeStringTransform, DtypeDateTransform

from datawarehouse.etl.transform import TrimRowsObject
from datawarehouse.models.Comercial import ClientesEntity
from datawarehouse.utils.general_functions import load_sql_statement
from datawarehouse.models.Comercial.articulos_entity import ArticulosEntity
from datawarehouse.common import get_session


def ejecutar_pipeline():
    query_name = "clientes_fenix.sql"
    with get_session("LOCAL") as session:
        tuple_codigos_articulos = ArticulosEntity.get_all_codigos_articulos(session)

    query = load_sql_statement(query_name)
    params = {'tuple_codart': tuple_codigos_articulos}

    columns_str = [
        'cod_cliente',
        'cliente',
        'cif'
    ]
    columns_int = [
        'id'
    ]

    columns_datetime = [
        'creation_date',
        'update_date'
    ]

    pipeline = LoggingPipeline([
        ('extractor database', DatabaseExtractor(db_alias='FENIX', query=query, params=params)),
        ('Transform Data Type',DtypeIntegerTransform(columns_int)),
        ('Transfomr Data String', DtypeStringTransform(columns_str)),
        ('Transform to DateTime', DtypeDateTransform(columns_datetime)),
        ('Delete Black Space start/End', TrimRowsObject()),
        ('load to dw', DWLoader(db_alias='LOCAL', model_class=ClientesEntity, mode="IGNORE", conflict_cols=["cod_cliente"]))
    ])

    pipeline.fit_transform(None)


def ejecutar_pipeline_by_codcli(tuple_cif_clients):
    query_name = "clientes_fenix_by_codcli.sql"

    query = load_sql_statement(query_name)
    params = {'tuple_cif_clients': tuple_cif_clients}

    columns_str = [
        'cod_cliente',
        'cliente',
        'cif'
    ]
    columns_int = [
        'id'
    ]

    columns_datetime = [
        'creation_date',
        'update_date'
    ]

    pipeline = LoggingPipeline([
        ('extractor database', DatabaseExtractor(db_alias='FENIX', query=query, params=params)),
        ('Transform Data Type',DtypeIntegerTransform(columns_int)),
        ('Transfomr Data String', DtypeStringTransform(columns_str)),
        ('Transform to DateTime', DtypeDateTransform(columns_datetime)),
        ('Eliminar Registros no necesario de Insertar', IdentifyChangeNewClients(tuple_cif_clients)),
        ('Delete Black Space start/End', TrimRowsObject()),
        ('load to dw', DWLoader(db_alias='LOCAL', model_class=ClientesEntity, mode="UPDATE", conflict_cols=["cif"]))
    ]
    )

    pipeline.fit_transform(None)

