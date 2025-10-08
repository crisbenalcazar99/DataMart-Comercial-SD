from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline
from sqlalchemy import and_

from common.session_manager import get_session
from etl.extract.db_extractor import DatabaseExtractor
from etl.load.db_load_test import DWLoader
from models.Comercial.articulos_entity import ArticulosEntity
from models.Comercial.facturas_entity import FacturasEntity
from models.Comercial.transacciones_entity import TransaccionesEntity
from pipelines.comercial.dim_clientes import ejecutar_pipeline_by_codcli
from utils.general_functions import load_sql_statement, list_in_string
from etl.transform.dtypes_massive import DtypeStringTransform, DtypeFloatTransform, DtypeCategoricalTransform, \
    DtypeDateTransform, DtypeIntegerTransform
from etl.transform.general_functions import RenameColumnsTransform, DropDuplicatesTransform, UpperLetterTransform, \
    ReplaceTextTransform, TrimRowsObject, DropColumns
from etl.transform.comercial_functions import IdentificarVendedorComentario, FacturaPagadaTransform, \
    IdentificarVendedorReasignaciones, DeleteVendedorNoComercial, CalculoSubtotalAbonado, FetchClientIdTransform, \
    FetchArticuloIdTransform, apply_filter_to_query


class FactTable:
    columns_str = [
        'numfac',
        'numdoc',
        'comen2',
        'comen3',
        'numero_factura',
        'cif',
        'cod_articulo'
    ]

    columns_int = [
        'id_tranfac',
        'id_factura',
        'cantidad_articulo',
        'id_vendedor'
    ]

    columns_float = [
        'subtotal_factura',
        'total_factura',
        'iva',
        'saldo_factura',
        'subtotal_articulo',
        'total_retencion',
        'total_abonado'
    ]

    columns_datetime = [
        'fecha_emision',
        'fecha_abono'
    ]

    columns_dim_facturas = [
        'id_factura',
        'numfac',
        'numdoc',
        'comen2',
        'comen3',
        'numero_factura',
        'fecha_emision',
        'fecha_abono',
        'subtotal_factura',
        'total_factura',
        'iva',
        'saldo_factura',
        'total_retencion',
        'total_abonado'

    ]
    columns_fact_transacciones = [
        'id_tranfac',
        'id_factura',
        'cantidad_articulo',
        'subtotal_articulo',
        'comen3',
        'fecha_emision',
        'id_vendedor',
        'cif',
        'cod_articulo'
    ]

    @classmethod
    def load_transaccciones_facturas(cls):
        query_path_otros_productos = r"C:\Users\cbenalcazar\Downloads\DataWarehouseSD\Consultas SQL\facturas_acticulos_fenix.sql"
        query_path_firmas_sf = r"C:\Users\cbenalcazar\Downloads\DataWarehouseSD\Consultas SQL\facturas_acticulos_fenix_firmas.sql"

        df_firmas_sf = cls.extract_data(
            query_path=query_path_firmas_sf,
            where_func=lambda p: p.filter(
                and_(
                    ArticulosEntity.producto.in_([32, 35]),
                    ArticulosEntity.empresa_origen == 50
                )

            )
        )
        df_otro_productos = cls.extract_data(
            query_path=query_path_otros_productos,
            where_func=lambda q: q.filter(
                and_(
                    ArticulosEntity.producto.notin_([32, 35]),
                    ArticulosEntity.empresa_origen == 50
                )

            )
        )

        df = pd.concat([df_otro_productos, df_firmas_sf], ignore_index=True)
        # EL Bulk de Update Date no es capaz de reconocer Valores NaT, np.nan mapea ese valor y lo tranformea a
        # None que es Similar a Null. El pd.notnull no tuvo esa capacidad, dejaba el NaT de igual manera.
        df = df.replace({np.nan: None})
        ejecutar_pipeline_by_codcli(tuple(df['cif'].unique()))
        cls.load_facturas(df)
        cls.load_transacciones(df)

    @classmethod
    def extract_data(cls, query_path, where_func=None):

        with get_session("LOCAL") as session:
            tuple_codart_filter = ArticulosEntity.get_list_codigos_articulos(
                session=session,
                where_func=where_func
            )
            max_transaction_date = FacturasEntity.get_last_transaction_date(
                session=session
            )
            max_payment_date = FacturasEntity.get_last_payment_date(
                session=session
            )

        query = load_sql_statement(query_path)
        query, params = apply_filter_to_query(query=query,
                                              tx_date=max_transaction_date,
                                              pay_date=max_payment_date,
                                              tuple_codart=tuple_codart_filter
                                              )

        pipeline = Pipeline([
            ('extractor database', DatabaseExtractor(db_alias='FENIX', query=query, params=params)),
            ('transform dtypes string', DtypeStringTransform(cls.columns_str)),
            ('transform dtypes float', DtypeFloatTransform(cls.columns_float)),
            ('transform dtypes datetime', DtypeDateTransform(cls.columns_datetime)),
            ('transform dtypes integer', DtypeIntegerTransform(cls.columns_int)),
            ('Delete Black Space start/End', TrimRowsObject())
        ])

        return pipeline.fit_transform(None)

    @classmethod
    def load_facturas(cls, df_original):
        df_original.info()
        df_facturas = df_original[cls.columns_dim_facturas].copy()

        pipeline = Pipeline([
            ('Rename Columns df', RenameColumnsTransform(
                dict_names={'id_factura': 'id', 'subtotal_factura': 'subtotal', 'total_factura': 'total'})),
            ('Drop Duplicates Facturas id', DropDuplicatesTransform(subset=['id'])),
            ('Identificar si la factura esta pagada', FacturaPagadaTransform('saldo_factura')),
            ('Calcular Subtotal Abonado', CalculoSubtotalAbonado('total_abonado', 'iva')),
            ('Carga la info en la DataBase',
             DWLoader(db_alias="LOCAL", model_class=FacturasEntity, conflict_cols=["numfac"], mode="UPDATE"))
        ])
        pipeline.fit_transform(df_facturas)

    @classmethod
    def load_transacciones(cls, df_original):
        df_transacciones = df_original[cls.columns_fact_transacciones].copy()

        pipeline = Pipeline([
            ('Rename Coluns ds', RenameColumnsTransform(dict_names={'id_tranfac': 'id'})),
            ('Establecer el id de vendedor en caso que el mismo factura', DeleteVendedorNoComercial('id_vendedor')),
            ('Identificar vendedor por Comen3', IdentificarVendedorComentario('comen3')),
            ('Fetch Client Id from Clients Table', FetchClientIdTransform('cif')),
            ('Fetch Articulo Id from Clients Table', FetchArticuloIdTransform('cod_articulo')),
            ('Identificar Vendedor Reasignaciones',
             IdentificarVendedorReasignaciones('id_cliente', 'fecha_emision', 'id_vendedor')),
            ("Eliminar Columnas no relevantes para el DB", DropColumns(['cif', 'cod_articulo'])),
            ('Carga la info en la Database',
             DWLoader(db_alias="LOCAL", model_class=TransaccionesEntity, conflict_cols=["id"], mode="UPDATE"))
        ])
        df_transacciones.info()
        pipeline.transform(df_transacciones)
