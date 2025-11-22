from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Sequence, Type

import pandas as pd

from datawarehouse.config.logging_pipeline import LoggingPipeline
from datawarehouse.etl.load.db_load_test import DWBatchedLoader
from datawarehouse.etl.transform.general_functions import (
    FilterColumns,
    SortValues,
    RenameColumnsTransform,
)
from datawarehouse.etl.transform.operatividad_functions import LastPurchaseByUserProduct, AddRucAuxiliar
from datawarehouse.models.zoho.fact_servicios_activos import FactServiciosActivosEntity


@dataclass(frozen=True)
class DatabaseLoaderPipelineConfig:
    # Config del loader
    db_alias: str = "QUANTA"
    model_class: Type = FactServiciosActivosEntity
    mode: str = "IGNORE"  # Supuesto razonable; ajusta a INSERT/IGNORE si lo necesitas

    # Conflic Columns para Loader DW
    conflict_cols: tuple[str] = ("id_user", "producto", "tipo_firma", "id_ruc_aux", 'serial_firma')


@dataclass(frozen=True)
class CurrentProductsPipelineConfig:
    """
    Configuración declarativa del pipeline de servicios/productos activos.
    """
    # Columnas a conservar del DF original
    COLUMNS_KEEP: Sequence[str] = (
        "producto",
        "medio",
        "id_ruc",
        "tipo_firma",
        "serial_firma",
        "fecha_caducidad",
        "id_user",
        "fecha_emision",
        "vigencia",
        'operador_creacion',
        "link_renovacion",
        "id_tramite"
    )

    # Ordenamiento para seleccionar el registro 'actual'
    sort_by: Sequence[str] = ("id_user", "id_ruc_aux", "producto", "fecha_caducidad")
    ascending: bool = False  # False = descendente

    # Claves únicas para deduplicar
    unique_keys: Sequence[str] = ("id_user", "id_ruc_aux", "producto")

    # Renombrado de columnas
    rename_map: Dict[str, str] = field(default_factory=lambda: {
        "fecha_caducidad": "fecha_caducidad_max",
    })

    # Config de Funcion de validacion ultima compra
    column_user = 'id_user'
    column_ruc = 'id_ruc'
    column_product = 'producto'
    column_tipo_firma = 'tipo_firma'
    column_ruc_aux = 'id_ruc_aux'

    # Nombre legible para logs
    pipeline_name: str = "Pipeline Servicios Activos → DW"


class CurrentProductsPipeline:
    """
    Pipeline modular para preparar y cargar los servicios/productos activos al DW.
    Pasos:
      1) Filtrado de columnas relevantes
      2) Ordenamiento por usuario, RUC, producto y fecha de caducidad
      3) Deduplicación para quedarnos con un registro por (id_user, ruc, producto)
      4) Renombrado de columnas (fecha_caducidad → fecha_caducidad_max)
      5) Carga a DW
    """

    def __init__(
            self,
            config: Optional[CurrentProductsPipelineConfig] = None,
            config_database: Optional[DatabaseLoaderPipelineConfig] = None
    ) -> None:
        self.config = config or CurrentProductsPipelineConfig()
        self.config_database = config_database or DatabaseLoaderPipelineConfig()
        self._pipeline_transform = self._build_transform_pipeline()

    def _build_transform_pipeline(self) -> LoggingPipeline:
        steps = [
            ("Filtrar columnas definidas", FilterColumns(list(self.config.COLUMNS_KEEP))),
            ('Agregar Ruc Auxiliar',
             AddRucAuxiliar(self.config.column_ruc, self.config.column_tipo_firma, self.config.column_ruc_aux)),
            (f"Sort By specify columns: {self.config.sort_by}",
             SortValues(list(self.config.sort_by), self.config.ascending)),
            (
                f"Eliminar redundancias por {self.config.unique_keys}",
                LastPurchaseByUserProduct(
                    column_user=self.config.column_user,
                    column_ruc=self.config.column_ruc,
                    column_product=self.config.column_product,
                    column_tipo_firma=self.config.column_tipo_firma
                )),
            ("Renombrar columnas", RenameColumnsTransform(dict_names=dict(self.config.rename_map))),

            ("Carga en DW", DWBatchedLoader(
                    db_alias=self.config_database.db_alias,
                    model_class=self.config_database.model_class,
                    mode=self.config_database.mode,
                    conflict_cols=list(self.config_database.conflict_cols),
                    batch_size=2500,
                    commit_per_batch=True
                )
            )
        ]

        return LoggingPipeline(steps=steps, pipeline_name=self.config.pipeline_name)

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ejecuta el pipeline completo sobre el DataFrame de entrada.
        Retorno: DataFrame transformado (por si se quiere seguir encadenando).
        """
        df_trans = self._pipeline_transform.fit_transform(df)
        return df_trans
