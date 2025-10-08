import pandas as pd

from etl.extract.db_extractor import DatabaseExtractor
from etl.load.db_load_test import DWLoader
from etl.transform.comercial_functions import FetchClientIdTransform
from etl.transform.dtypes_massive import DtypeIntegerTransform, DtypeStringTransform, DtypeDateTransform
from sklearn.pipeline import Pipeline

from etl.transform.general_functions import TrimRowsObject, SortValues, DropDuplicatesTransform
from models.Comercial.clientes_entity import ClientesEntity
from models.Comercial.reasignaciones_entity import ReasignacionesEntity
from utils.general_functions import load_sql_statement, list_in_string
from models.Comercial.articulos_entity import ArticulosEntity
from common.session_manager import get_session

def ejecutar_pipeline():
    query_path = ''

    query = load_sql_statement(query_path)

    columns_str = [
        'ruc',
        'vendedor'
    ]

    columns_datetime = [
        'creation_date'
    ]

    pipeline = Pipeline([
        ('extractor database', DatabaseExtractor(db_alias="PORTAL", query=query)),
        ('Transform Data String', DtypeStringTransform(columns_str)),
        ('Transform dateTime', DtypeDateTransform(columns_datetime)),
        ('Sort Values by creation_date', SortValues(columns_datetime)),
        ('Trim Values', TrimRowsObject()),
        ('Drop Duplicates in base RUC', DropDuplicatesTransform(['ruc'])),
        ('Fetch Client Id from Clients Table', FetchClientIdTransform('ruc'))

    ])

def preload_reasignaciones():
    df_reasignaciones = pd.read_excel(r"C:\Users\cbenalcazar\Downloads\DataWarehouseSD\archivos\reasignaciones_precarga.xlsx", dtype={
        'ruc': 'string'
    })
    for index, row in df_reasignaciones.iterrows():
        with get_session("LOCAL") as session:
            id_cliente = ClientesEntity.get_cliente_id(
                session=session,
                where_func=lambda q: q.filter(
                    ClientesEntity.cif == row['ruc']
                )
            )
        df_reasignaciones.at[index, 'id_cliente'] = id_cliente
    df_reasignaciones.drop(columns=['ruc', 'vendedor'], inplace=True)
    df_reasignaciones['id_cliente'] = df_reasignaciones['id_cliente'].astype('Int64')
    df_reasignaciones.info()
    dwloader = DWLoader("LOCAL", ReasignacionesEntity)

    dwloader.fit_transform(df_reasignaciones)
