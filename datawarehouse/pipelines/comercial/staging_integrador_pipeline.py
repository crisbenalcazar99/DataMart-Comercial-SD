from typing import Dict, List, Optional, Literal

import pandas as pd
from sqlalchemy import and_

from dataclasses import dataclass

from datawarehouse.common.session_manager import get_session
from datawarehouse.config.logging_pipeline import LoggingPipeline
from datawarehouse.etl.extract.db_extractor import DatabaseExtractorSQLServer
from datawarehouse.etl.transform.comercial_functions import AddTipoVendedorColumn
from datawarehouse.models.Comercial.dim_articulos_entity import DimArticulosEntity

from datawarehouse.utils.RunMode import RunMode
from datawarehouse.utils.general_functions import load_sql_statement_comercial
from datawarehouse.etl.transform.dtypes_massive import DtypeStringTransform, DtypeFloatTransform, DtypeDateTransform, \
    DtypeIntegerTransform
from datawarehouse.etl.transform.general_functions import TrimRowsObject, ConcatDataFrames
import logging


@dataclass(frozen=True)
class DatabaseLoaderPipelineConfig:
    # Config del loader
    db_alias: str = "QUANTA"
    mode: str = "UPDATE"  # Supuesto razonable; ajusta a INSERT/IGNORE si lo necesitas


@dataclass(frozen=True)
class SourceSpec:
    name: Literal["fenix_firmas", "fenix_otros_productos", "fenix_TV", "latinum"]
    db_alias: Literal["FENIX", "LATINUM"]
    query_file: str  # Nombre del archivo .sql


class StagingIntegradorComercial:
    logger = logging.getLogger(__name__)

    # Especificación de fuentes por modo
    SOURCES_BY_MODE: Dict[RunMode, List[SourceSpec]] = {
        RunMode.INICIAL: [
            SourceSpec("fenix_firmas", "FENIX", "facturas_acticulos_fenix.sql"),
            SourceSpec("fenix_otros_productos", "FENIX", "facturas_acticulos_fenix_firmas.sql", ),
            SourceSpec("fenix_TV", "FENIX", "facturas_acticulos_fenix_TV.sql", ),
            SourceSpec("latinum", "LATINUM", "facturas_acticulos_latinum.sql"),
        ],
        RunMode.INCREMENTAL: [

        ],
    }

    COLUMNS_STR = [
        'codcli',
        'nom_cliente',
        'cif',
        'cod_articulo',
        'numfac',
        'numdoc',
        'comen2',
        'comen3',
        'numero_factura',
        'id_transaction',
        'estado_factura'
    ]

    COLUMNS_INTEGER = [
        'id_vendedor',
        'cantidad_articulo',
    ]

    COLUMNS_FLOAT = [
        'subtotal',
        'total',
        'iva',
        'saldo_factura',
        'total_retencion',
        'total_abonado',
        'subtotal_articulo',
    ]

    COLUMNS_DATETIME = [
        'fecha_emision',
        'fecha_abono',
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
        'total_abonado',

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
    def run(cls, mode: RunMode = RunMode.INICIAL) -> pd.DataFrame:
        """
        Ejecuta el pipeline de integración en modo INICIAL o INCREMENTAL.
        """
        folder = "inicializacion" if mode is RunMode.INICIAL else "actualizacion_incremental"

        # Construir y ejecutar un pipeline por fuente
        frames: List[pd.DataFrame] = []
        for spec in cls.SOURCES_BY_MODE[mode]:
            params = cls._build_params_for_mode(mode, spec)
            query = load_sql_statement_comercial(folder, spec.query_file)
            pipe = cls._build_source_pipeline(spec, query, params)
            df = pipe.fit_transform(None)

            frames.append(df)

        # Integración final
        df = cls._integration_pipeline().fit_transform(frames)
        return df

    # --------------------------
    # Helpers de construcción
    # --------------------------
    @classmethod
    def _build_params_for_mode(cls, mode: RunMode, spec: SourceSpec) -> Optional[Dict[str, object]]:
        if mode is RunMode.INICIAL and spec.name == 'fenix_firmas':
            with get_session(DatabaseLoaderPipelineConfig.db_alias) as session:
                params = DimArticulosEntity.get_list_codigos_articulos(
                    session=session,
                    where_func=lambda p: p.filter(
                        and_(
                            DimArticulosEntity.producto.notin_([32, 35]),
                            DimArticulosEntity.empresa_origen == 50
                        )
                    )
                )
            return {"tuple_codart": params}
        elif mode is RunMode.INICIAL and spec.name == 'fenix_otros_productos':
            with get_session(DatabaseLoaderPipelineConfig.db_alias) as session:
                params = DimArticulosEntity.get_list_codigos_articulos(
                    session=session,
                    where_func=lambda p: p.filter(
                        and_(
                            DimArticulosEntity.producto.in_([32, 35]),
                            DimArticulosEntity.empresa_origen == 50
                        )
                    )
                )
            return {"tuple_codart": params}

        elif mode is RunMode.INICIAL and spec.name == 'fenix_TV':
            with get_session(DatabaseLoaderPipelineConfig.db_alias) as session:
                params = DimArticulosEntity.get_list_codigos_articulos(
                    session=session,
                    where_func=lambda p: p.filter(
                        and_(
                            DimArticulosEntity.producto.in_([32]),
                            DimArticulosEntity.empresa_origen == 50
                        )
                    )
                )
            return {"tuple_codart": params}

        elif mode is RunMode.INICIAL and spec.name == 'latinum':
            with get_session(DatabaseLoaderPipelineConfig.db_alias) as session:
                params = DimArticulosEntity.get_list_codigos_articulos(
                    session=session,
                    where_func=lambda p: p.filter(DimArticulosEntity.empresa_origen == 51)
                )
            return {"tuple_codart": params}
        return None

        # # Para incremental, obtenemos la max fecha de aprobación
        # with get_session("QUANTA") as session:
        #     max_fecha_aprobacion = FactTransaccionesOperatividadEntity.get_last_transaction_date(session=session)
        # return {"max_fecha_aprobacion": max_fecha_aprobacion}

    @classmethod
    def _build_source_pipeline(cls, spec: SourceSpec, sql_text: str,
                               params: Optional[Dict[str, object]]) -> LoggingPipeline:
        """
        Pipeline de extracción + tipado para UNA fuente.
        """
        return LoggingPipeline(
            steps=[
                ("extractor database", DatabaseExtractorSQLServer(db_alias=spec.db_alias, query=sql_text, params=params)),
                ("Transform String", DtypeStringTransform(cls.COLUMNS_STR, 255)),
                ("Transform Datetime", DtypeDateTransform(cls.COLUMNS_DATETIME)),
                ("Transform Integer", DtypeIntegerTransform(cls.COLUMNS_INTEGER)),
                ("Transform Floats", DtypeFloatTransform(cls.COLUMNS_FLOAT)),
                ('Delete Black Space start/End', TrimRowsObject()),
                ('Add Tipo Vendedor Column', AddTipoVendedorColumn(spec.name)),
            ],
            pipeline_name=f"Pipeline Extract {spec.name.upper()}",
        )

    @classmethod
    def _integration_pipeline(cls) -> LoggingPipeline:
        """
        Pipeline de integración/normalización común a todas las fuentes.
        """
        # Replaces declarativos generados dinámicamente
        replace_steps = []

        steps = [
            ("Union de varios DF", ConcatDataFrames()),
        ]

        return LoggingPipeline(steps=steps, pipeline_name="Pipeline Integración y Normalización")
