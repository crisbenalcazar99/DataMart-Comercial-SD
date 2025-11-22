from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

from datawarehouse.common.session_manager import get_session
from datawarehouse.config.logging_pipeline import LoggingPipeline
from datawarehouse.etl.extract.db_extractor import DatabaseExtractor
from datawarehouse.etl.transform.dtypes_massive import DtypeStringTransform, DtypeDateTransform, DtypeBooleanTransform, \
    DtypeIntegerTransform
from datawarehouse.etl.transform.general_functions import (
    ConcatDataFrames,
    DropDuplicatesTransform,
    ReplaceTextTransform,
    RenameColumnsTransform, TrimRowsObject, SortValues, AddConstantColumn,
)
from datawarehouse.etl.transform.operatividad_functions import GenerateLinkAutorenovacion
from datawarehouse.models.zoho.fact_transacciones_operatividad import FactTransaccionesOperatividadEntity
from datawarehouse.utils.RunMode import RunMode
from datawarehouse.utils.general_functions import load_sql_statement_operatividad



@dataclass(frozen=True)
class SourceSpec:
    name: str               # Alias humano: "subca1", "subca2", "camunda"
    db_alias: str           # "PORTAL" o "CAMUNDA"
    query_file: str         # Nombre del archivo .sql
    link_emisor: str        # Configuracion para Link de Renovacion
    link_so: str            # Configuracion para Link de Renovacion


class StagingIntegradorOperatividad:
    """
    Clase modular y DRY para ejecutar la integración de datos desde 3 fuentes
    (SubCA1, SubCA2, Camunda), en modo inicial o incremental.
    """

    # Columnas para los casteos
    COLUMNS_STR: List[str] = [
        "cedula", "vigencia", "producto", "mediocam", "razon_social", "ruc",
        "tipo_firma", "estado_firma", "serial_firma", "correo", "telefono", "nombre", "apellido_paterno",
        "apellido_materno", "operador_creacion", "profesion", "clase_contribuyente", "sector_economico", "actividad_ruc"
    ]
    COLUMNS_STR_128: List[str] = [
        "apellido_paterno", "apellido_materno", "nombre"
    ]

    COLUMNS_STR_32: List[str] = [
        "cedula", "ruc"
    ]

    COLUMNS_INTEGER: List[str] = [
        "id_tramite", "link_id_firma"
    ]

    COLUMNS_DATETIME: List[str] = ["fecha_aprobacion", "fecha_caducidad", "fecha_emision", "fecha_nacimiento"]

    COLUMNS_BOOLEAN: List[str] = ["contribuyente_fantasma"]

    # Declaración de reemplazos comunes (mapeos declarativos)
    INVALID_TO_NULL_COLUMNS: Dict[str, List[str]] = {
        "ruc": ["", "None", "-"],
        "razon_social": ["", "None"],
    }

    PRODUCT_NORMALIZATION: Dict[str, str] = {
        "Emision": "FIRMA ELECTRONICA",
        "Renovacion": "FIRMA ELECTRONICA",
        "Recuperacion Clave": "FIRMA ELECTRONICA",
        "Agregar RUC a Firma": "FIRMA ELECTRONICA",
        "Emision SF sin Firma": "SF SIN FIRMA",
        "Emision SF": "SF CON FIRMA",
        "Renovacion SF": "SF CON FIRMA",
    }

    # Especificación de fuentes por modo
    SOURCES_BY_MODE: Dict[RunMode, List[SourceSpec]] = {
        RunMode.INICIAL: [
            SourceSpec("subca1", "PORTAL", "subca1.sql", "P_SUBCA1", "SO_PORTALES"),
            SourceSpec("subca2", "PORTAL", "subca2.sql", "P_SUBCA2", "SO_PORTALES"),
            SourceSpec("camunda", "CAMUNDA", "camunda.sql", "P_CAMUNDA", "SO_CAMUNDA"),
        ],
        RunMode.INCREMENTAL: [
            #SourceSpec("subca1", "PORTAL", "subca1_incremental.sql"),
            #SourceSpec("subca2", "PORTAL", "subca2_incremental.sql"), #Se desactivan puesto que estos servidores ya no reciben nuevos registros
            SourceSpec("camunda", "CAMUNDA", "camunda_incremental.sql", "P_CAMUNDA", "SO_CAMUNDA"),
        ],
    }

    @classmethod
    def run(cls, mode: RunMode = RunMode.INICIAL) -> pd.DataFrame:
        """
        Ejecuta el pipeline de integración en modo INICIAL o INCREMENTAL.
        """
        folder = "inicializacion" if mode is RunMode.INICIAL else "actualizacion_incremental"
        params = cls._build_params_for_mode(mode)

        # Construir y ejecutar un pipeline por fuente
        frames: List[pd.DataFrame] = []
        for spec in cls.SOURCES_BY_MODE[mode]:
            query = load_sql_statement_operatividad(folder, spec.query_file)
            pipe = cls._build_source_pipeline(spec, query, params)
            df = pipe.fit_transform(None)
            frames.append(df)

        # Integración final
        df = cls._integration_pipeline().fit_transform(frames)
        # df = df[df['fecha_nacimiento'].notna()]
        return df
        #return df

    # --------------------------
    # Helpers de construcción
    # --------------------------
    @classmethod
    def _build_params_for_mode(cls, mode: RunMode) -> Optional[Dict[str, object]]:
        if mode is RunMode.INICIAL:
            return None
        # Para incremental, obtenemos la max fecha de aprobación
        with get_session("QUANTA") as session:
            max_fecha_aprobacion = FactTransaccionesOperatividadEntity.get_last_transaction_date(session=session)
        return {"max_fecha_aprobacion": max_fecha_aprobacion}

    @classmethod
    def _build_source_pipeline(cls, spec: SourceSpec, sql_text: str, params: Optional[Dict[str, object]]) -> LoggingPipeline:
        """
        Pipeline de extracción + tipado para UNA fuente.
        """
        return LoggingPipeline(
            steps=[
                ("extractor database", DatabaseExtractor(db_alias=spec.db_alias, query=sql_text, params=params)),
                ("Transform String", DtypeStringTransform(cls.COLUMNS_STR, 255)),
                ("Transform String", DtypeStringTransform(cls.COLUMNS_STR_128, 128)),
                ("Transform String", DtypeStringTransform(cls.COLUMNS_STR_32, 32)),
                ("Transform Datetime", DtypeDateTransform(cls.COLUMNS_DATETIME)),
                ("Transform Boolean", DtypeBooleanTransform(cls.COLUMNS_BOOLEAN)),
                ("Transform Integer", DtypeIntegerTransform(cls.COLUMNS_INTEGER)),
                ('Delete Black Space start/End', TrimRowsObject()),
                ('Add columns para link', AddConstantColumn('link_emisor', spec.link_emisor)),
                ('Add columns para link', AddConstantColumn('link_so', spec.link_so))
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

        # Normalización de 'producto'
        for old, new in cls.PRODUCT_NORMALIZATION.items():
            replace_steps.append(
                (f"Normalizar producto: {old} → {new}", ReplaceTextTransform(old, new, "producto"))
            )

        # Relleno de NULLs (ruc / razon_social)
        for column, invalid_values in cls.INVALID_TO_NULL_COLUMNS.items():
            for inv in invalid_values:
                replace_steps.append(
                    (f"Normalizar {column}: '{inv}' → NULL", ReplaceTextTransform(inv, pd.NA, column))
                )

        steps = [
            ("Union de varios DF", ConcatDataFrames()),
            ("Eliminar duplicados por 'serial_firma'", DropDuplicatesTransform(["serial_firma"])),
            ("Renombrar mediocam → medio", RenameColumnsTransform(dict_names={"mediocam": "medio"})),
            *replace_steps,
            ("Generar el link", GenerateLinkAutorenovacion()),
            ('Ordenar el DF por Fecha Aprobacion', SortValues('fecha_aprobacion', False))
        ]

        return LoggingPipeline(steps=steps, pipeline_name="Pipeline Integración y Normalización")


class CambioEstadoCertificados:
    # Columnas para los casteos
    COLUMNS_INTEGER: List[str] = ["id_firma_subida"]
    COLUMNS_STR:List[str] = ["estado_certificado"]
