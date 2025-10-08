from etl.extract.db_extractor import DatabaseExtractor
from etl.load.db_load_test import DWLoader
from etl.transform.dtypes_massive import DtypeIntegerTransform, DtypeStringTransform
from sklearn.pipeline import Pipeline
from models.Comercial.vendedores_entity import VendedoresEntity
from utils.general_functions import load_sql_statement, list_in_string


def ejecutar_pipeline():
    query_name = "vendedores_fenix.sql"
    query = load_sql_statement(query_name)

    columns_int = [
        "id",
        "cod_vendedor"
    ]

    pipeline = Pipeline([
        ('extractor database', DatabaseExtractor(db_alias='FENIX', query=query)),
        ('Transform Data Type', DtypeIntegerTransform(columns_int)),
        ('load to dw', DWLoader(db_alias='LOCAL', model_class=VendedoresEntity, mode="IGNORE", conflict_cols=["id"]))
    ])

    pipeline.fit_transform(None)
