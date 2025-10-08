from etl.extract.db_extractor import DatabaseExtractor
from etl.load.db_load_test import DWLoader
from etl.transform.dtypes_massive import DtypeIntegerTransform, DtypeStringTransform
from sklearn.pipeline import Pipeline
from models.Comercial.vendedores_entity import VendedoresEntity
from utils.general_functions import load_sql_statement, list_in_string
from models.Comercial.articulos_entity import ArticulosEntity
from common.session_manager import get_session


def ejecutar_pipeline():
    query_path = r"C:\Users\cbenalcazar\Downloads\DataWarehouseSD\Consultas SQL\vendedores_fenix.sql"
    query = load_sql_statement(query_path)


    columns_int = [
        "id",
        "cod_vendedor"
    ]

    pipeline = Pipeline([
        ('extractor database', DatabaseExtractor(db_alias='FENIX', query=query)),
        ('Transform Data Type',DtypeIntegerTransform(columns_int)),
        ('load to dw', DWLoader(db_alias='LOCAL', model_class=VendedoresEntity, mode="IGNORE", conflict_cols=["id"]))
    ])

    pipeline.fit_transform(None)