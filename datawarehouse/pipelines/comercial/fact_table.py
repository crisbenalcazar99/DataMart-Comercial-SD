import numpy as np
import pandas as pd
from sqlalchemy import and_

from datawarehouse.common import get_session
from datawarehouse.config import LoggingPipeline
from datawarehouse.etl.extract import DatabaseExtractor
from datawarehouse.etl.load.db_load_test import DWLoader
from datawarehouse.models.Comercial.articulos_entity import ArticulosEntity
from datawarehouse.models.Comercial.facturas_entity import FacturasEntity
from datawarehouse.models.Comercial.transacciones_entity import TransaccionesEntity
from datawarehouse.pipelines.comercial.dim_clientes import ejecutar_pipeline_by_codcli
from datawarehouse.utils.general_functions import load_sql_statement
from datawarehouse.etl.transform import DtypeStringTransform, DtypeFloatTransform, DtypeDateTransform, DtypeIntegerTransform
from datawarehouse.etl.transform import RenameColumnsTransform, DropDuplicatesTransform, TrimRowsObject, DropColumns
from datawarehouse.etl.transform import IdentificarVendedorComentario, FacturaPagadaTransform, \
    IdentificarVendedorReasignaciones, DeleteVendedorNoComercial, CalculoSubtotalAbonado, FetchClientIdTransform, \
    FetchArticuloIdTransform, apply_filter_to_query
import logging


class FactTable:
    logger = logging.getLogger(__name__)

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
        cls.logger.info("Inicio del proceso de cagra de oinformacion test ")
        query_name_otros_productos = "facturas_acticulos_fenix.sql"
        query_name_firmas_sf = "facturas_acticulos_fenix_firmas.sql"

        df_firmas_sf = cls.extract_data(
            query_name=query_name_firmas_sf,
            where_func=lambda p: p.filter(
                and_(
                    ArticulosEntity.producto.in_([32, 35]),
                    ArticulosEntity.empresa_origen == 50
                )

            )
        )
        df_otro_productos = cls.extract_data(
            query_name=query_name_otros_productos,
            where_func=lambda q: q.filter(
                and_(
                    ArticulosEntity.producto.notin_([32, 35]),
                    ArticulosEntity.empresa_origen == 50
                )

            )
        )

        print(df_otro_productos.info())

        df = pd.concat([df_otro_productos, df_firmas_sf], ignore_index=True)
        # EL Bulk de Update Date no es capaz de reconocer Valores NaT, np.nan mapea ese valor y lo tranformea a
        # None que es Similar a Null. El pd.notnull no tuvo esa capacidad, dejaba el NaT de igual manera.
        df = df.replace({np.nan: None})
        ejecutar_pipeline_by_codcli(tuple(df['cif'].unique()))
        cls.load_facturas(df)
        cls.load_transacciones(df)

    @classmethod
    def extract_data(cls, query_name, where_func=None):
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

        query = load_sql_statement(query_name)
        query, params = apply_filter_to_query(query=query,
                                              tx_date=max_transaction_date,
                                              pay_date=max_payment_date,
                                              tuple_codart=tuple_codart_filter
                                              )

        pipeline = LoggingPipeline([
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
        df_facturas = df_original[cls.columns_dim_facturas].copy()

        pipeline = LoggingPipeline([
            ('Rename Columns df', RenameColumnsTransform(
                dict_names={'id_factura': 'id', 'subtotal_factura': 'subtotal', 'total_factura': 'total'})),
            ('Drop Duplicates Facturas id', DropDuplicatesTransform(subset=['id'])),
            ('Identificar si la factura esta pagada', FacturaPagadaTransform('saldo_factura')),
            ('Calcular Subtotal Abonado', CalculoSubtotalAbonado('total_abonado', 'iva')),
            ('Carga la info en la DataBase',
             DWLoader(db_alias="LOCAL", model_class=FacturasEntity, conflict_cols=["numfac"], mode="UPDATE"))
        ], pipeline_name="Carga de Facturas")
        pipeline.fit_transform(df_facturas)

    @classmethod
    def load_transacciones(cls, df_original):
        df_transacciones = df_original[cls.columns_fact_transacciones].copy()

        pipeline = LoggingPipeline([
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
        pipeline.transform(df_transacciones)
