import pandas as pd

from datawarehouse.etl.extract import DatabaseExtractor
from datawarehouse.etl.load.db_load_test import DWLoader
from datawarehouse.etl.transform import FetchClientIdTransform
from datawarehouse.etl.transform import DtypeStringTransform, DtypeDateTransform
from sklearn.pipeline import Pipeline

from datawarehouse.etl.transform import TrimRowsObject, SortValues, DropDuplicatesTransform
from datawarehouse.models.Comercial import ClientesEntity
from datawarehouse.models.Comercial import ReasignacionesEntity
from datawarehouse.utils.general_functions import load_sql_statement
from datawarehouse.common import get_session

def ejecutar_pipeline():
    query_name = ''

    query = load_sql_statement(query_name)

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
    path_excel_file = et_proyect_root() / "archivos" / "reasignaciones_precarga.xlsx"
    df_reasignaciones = pd.read_excel(path_excel_file, dtype={
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
