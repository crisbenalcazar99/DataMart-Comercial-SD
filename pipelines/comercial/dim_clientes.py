from etl.extract.db_extractor import DatabaseExtractor
from etl.load.db_load_test import DWLoader
from etl.transform.comercial_functions import IdentifyChangeNewClients
from etl.transform.dtypes_massive import DtypeIntegerTransform, DtypeStringTransform, DtypeDateTransform
from sklearn.pipeline import Pipeline

from etl.transform.general_functions import TrimRowsObject
from models.Comercial.clientes_entity import ClientesEntity
from utils.general_functions import load_sql_statement, list_in_string
from models.Comercial.articulos_entity import ArticulosEntity
from common.session_manager import get_session


def ejecutar_pipeline():
    query_path = r"C:\Users\cbenalcazar\Downloads\DataWarehouseSD\Consultas SQL\clientes_fenix.sql"
    with get_session("LOCAL") as session:
        tuple_codigos_articulos = ArticulosEntity.get_all_codigos_articulos(session)

    query = load_sql_statement(query_path)
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

    pipeline = Pipeline([
        ('extractor database', DatabaseExtractor(db_alias='FENIX', query=query, params=params)),
        ('Transform Data Type',DtypeIntegerTransform(columns_int)),
        ('Transfomr Data String', DtypeStringTransform(columns_str)),
        ('Transform to DateTime', DtypeDateTransform(columns_datetime)),
        ('Delete Black Space start/End', TrimRowsObject()),
        ('load to dw', DWLoader(db_alias='LOCAL', model_class=ClientesEntity, mode="IGNORE", conflict_cols=["cod_cliente"]))
    ])

    pipeline.fit_transform(None)


def ejecutar_pipeline_by_codcli(tuple_cif_clients):
    query_path = r"C:\Users\cbenalcazar\Downloads\DataWarehouseSD\Consultas SQL\clientes_fenix_by_codcli.sql"

    query = load_sql_statement(query_path)
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

    pipeline = Pipeline([
        ('extractor database', DatabaseExtractor(db_alias='FENIX', query=query, params=params)),
        ('Transform Data Type',DtypeIntegerTransform(columns_int)),
        ('Transfomr Data String', DtypeStringTransform(columns_str)),
        ('Transform to DateTime', DtypeDateTransform(columns_datetime)),
        ('Eliminar Registros no necesario de Insertar', IdentifyChangeNewClients(tuple_cif_clients)),
        ('Delete Black Space start/End', TrimRowsObject()),
        ('load to dw', DWLoader(db_alias='LOCAL', model_class=ClientesEntity, mode="UPDATE", conflict_cols=["cif"]))
    ])

    pipeline.fit_transform(None)

