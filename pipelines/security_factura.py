from etl.extract.db_extractor import DatabaseExtractor
from etl.load.db_load_test import DWLoader
from etl.transform.dtypes_massive import DtypeStringTransform, DtypeFloatTransform, DtypeCategoricalTransform, \
    DtypeDateTransform

from sklearn.pipeline import Pipeline
from config.setting import DB_CONFIG_PORTAL
from common.session_manager import get_session

from models.testing_table import TestingBase

from common.schema_initializer import create_schema

create_schema()  # COmentar despues de la creacion del schema


def ejecutar_pipeline():
    query_path = r"C:\Users\cbenalcazar\Downloads\Consulta_SF_Portal.sql"
    parametro_fecha_aprobacion = '{{parametro_fecha_aprobacion}}'

    with get_session('local') as session:
        fecha_max = TestingBase.search_max_aprobation_date(session)

    if fecha_max is None:
        fecha_aprobacion = "'0000-00-00 00:00:00'"  # o una fecha base válida como '1900-01-01 00:00:00'
    else:
        fecha_aprobacion = f"'{fecha_max.strftime('%Y-%m-%d %H:%M:%S')}'"

    columns_str = [
        'cedula',
        'factura',
        'ruc',
        'razon_social',
        'operador_creacion',
        'serial_firma',
        'correo',
        'telefono',
        'nombre',
        'operador_aprobacion'
    ]

    columns_cat = [
        'portal_origen',
        'vigencia',
        'producto',
        'medio_firma',
        'tipo_persona',
        'medio_contacto',
        'flujo'
    ]

    columns_datetime = [
        'fecha_aprobacion',
        'fecha_expedicion',
        'fecha_facturacion',
        'fecha_inicio_tramite',
        'fecha_caducidad'
    ]

    columns_float = [
        'valor_factura'
    ]

    with open(query_path, 'r') as file:
        query = file.read()
        query = query.replace(parametro_fecha_aprobacion, fecha_aprobacion)

    pipeline = Pipeline([
        ('extractor database', DatabaseExtractor(db_alias='PORTAL', query=query)),
        ('transform dtypes string', DtypeStringTransform(columns_str)),
        ('transform dtypes float', DtypeFloatTransform(columns_float)),
        ('transform dtypes categorical', DtypeCategoricalTransform(columns_cat)),
        ('transform dtypes datetime', DtypeDateTransform(columns_datetime)),
        ('load to dw', DWLoader(db_alias='LOCAL', model_class=TestingBase))
    ])

    pipeline.fit_transform(None)



