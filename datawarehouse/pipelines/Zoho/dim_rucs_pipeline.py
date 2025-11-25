from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Type

import pandas as pd

from datawarehouse.config.logging_pipeline import LoggingPipeline
from datawarehouse.etl.load.db_load_test import DWBatchedLoader
from datawarehouse.etl.transform.general_functions import (
    FilterColumns,
    SortValues,
    DropDuplicatesTransform,
    DropColumns,
    CheckValueNotEqualFlag,
)

from datawarehouse.models.zoho.dim_rucs import DimRucsEntity


@dataclass(frozen=True)
class RucsPipelineConfig:
    """
    Configuración declarativa del pipeline de RUCs.
    """
    # Columnas a conservar del DF original
    columns_keep: Sequence[str] = (
        "razon_social",
        "ruc",
        "fecha_emision",
        "actividad_ruc",
        "clase_contribuyente",
        "sector_economico",
        "contribuyente_fantasma",
        "tipo_firma",
    )

    # Claves únicas para deduplicar
    unique_keys: Sequence[str] = ("ruc",)
    update_columns_conflict: tuple[str] = ('razon_social', 'actividad_ruc', 'clase_contribuyente', 'sector_economico',
                                           'contribuyente_fantasma', 'update_date')

    # Configuración del ordenamiento
    sort_by: Sequence[str] = ("fecha_emision",)
    ascending: bool = False  # False = descendente

    # Configuración del flag de empresa/persona
    id_column: str = "tipo_firma"
    personal_prefix: str = "PN"
    company_flag_column: str = "is_company"

    # Configuración del loader
    db_alias: str = "QUANTA"
    model_class: Type = DimRucsEntity
    mode: str = "UPDATE"  # INSERT | IGNORE | UPDATE
    conflict_cols: tuple[str] = ("ruc",)

    # Nombre legible para logs
    pipeline_name: str = "Pipeline RUCs → DW"


class RucsPipeline:
    """
    Pipeline modular para preparar y cargar RUCs al DW.
    Pasos:
      1) Filtrado de columnas relevantes
      2) Ordenamiento por fecha de emisión
      3) Deduplicación por RUC
      4) Marcado de RUC empresarial/personal
      5) Eliminación de columnas auxiliares
      6) Carga a DW (INSERT/UPDATE/IGNORE según config)
    """

    def __init__(self, config: Optional[RucsPipelineConfig] = None) -> None:
        self.config = config or RucsPipelineConfig()
        self._pipeline = self._build_pipeline()

    def _build_pipeline(self) -> LoggingPipeline:
        steps = [
            ("Filtrar columnas definidas", FilterColumns(list(self.config.columns_keep))),
            ("Ordenar por fecha_emision", SortValues(list(self.config.sort_by), self.config.ascending)),
            ("Eliminar redundancias por RUC", DropDuplicatesTransform(list(self.config.unique_keys))),
            ("Marcar RUC empresarial/personal",
                CheckValueNotEqualFlag(
                    self.config.id_column,
                    self.config.personal_prefix,
                    self.config.company_flag_column,
                )
            ),
            ("Eliminar columnas auxiliares", DropColumns(["fecha_emision", "tipo_firma"])),
            ("Carga en DW", DWBatchedLoader(
                    db_alias=self.config.db_alias,
                    model_class=self.config.model_class,
                    mode=self.config.mode,
                    conflict_cols=list(self.config.conflict_cols),
                    batch_size=3000,
                    commit_per_batch=True,
                    update_cols=self.config.update_columns_conflict
                )
            ),
        ]

        return LoggingPipeline(steps=steps, pipeline_name=self.config.pipeline_name)

    def run(self, df: pd.DataFrame) -> None:
        """
        Ejecuta el pipeline completo sobre el DataFrame de entrada.
        Retorno: None (la carga la realiza el DWLoader).
        """
        self._pipeline.fit_transform(df)