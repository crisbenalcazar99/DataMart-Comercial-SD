from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Type, Literal

import pandas as pd

from datawarehouse.config.logging_pipeline import LoggingPipeline
from datawarehouse.etl.load.db_load_test import DWBatchedLoader
from datawarehouse.etl.transform.general_functions import (
    FilterColumns,
)
from datawarehouse.etl.transform.operatividad_functions import (
    FetchAndAttachId,
)
from datawarehouse.models.zoho.dim_rucs import DimRucsEntity
from datawarehouse.models.zoho.fact_transacciones_operatividad import FactTransaccionesOperatividadEntity
from datawarehouse.models.zoho.dim_usuarios import DimUsuariosEntity
from datawarehouse.utils.RunMode import RunMode


@dataclass(frozen=True)
class TransactionHistoryPipelineConfig:
    """
    Configuración declarativa del pipeline de histórico de transacciones.
    """
    # Columnas a conservar del DF original
    columns_keep: Sequence[str] = (
        "cedula",
        "producto",
        "medio",
        "ruc",
        "tipo_firma",
        "serial_firma",
        "fecha_aprobacion",
        "fecha_caducidad",
        "fecha_emision",
        "vigencia",
        "operador_creacion",
        "id_tramite",
        "link_renovacion"
    )

    # Configuración de los transforms de enriquecimiento
    user_id_source_column: str = "cedula"
    ruc_identifier_column: str = "ruc"
    ruc_entity: Type = DimRucsEntity
    join_how_ruc: Literal["left", "right", "inner", "outer", "cross"] = "left"

    user_identifier_column: str = "cedula"
    user_entity: Type = DimUsuariosEntity
    join_how_user: Literal["left", "right", "inner", "outer", "cross"] = "inner"

    # Configuración del loader
    db_alias: str = "QUANTA"
    model_class: Type = FactTransaccionesOperatividadEntity
    mode: str = "IGNORE"  # INSERT | IGNORE | UPDATE
    conflict_cols: tuple[str] = ("serial_firma",)

    # Nombre legible para logs
    pipeline_name: str = "Pipeline Histórico Transacciones → DW"


class TransactionHistoryPipeline:
    """
    Pipeline modular para preparar y cargar el histórico de transacciones al DW.
    Pasos:
      1) Filtrado de columnas relevantes
      2) Enriquecer con id de usuario
      3) Enriquecer con id de RUC
      4) Carga a DW (INSERT/UPDATE/IGNORE según config)
    """

    def __init__(self, run_mode: RunMode = RunMode.INICIAL, config: Optional[TransactionHistoryPipelineConfig] = None, ) -> None:
        self.run_mode = run_mode
        self.config = config or TransactionHistoryPipelineConfig()
        self._pipeline = self._build_pipeline()

    def _build_pipeline(self) -> LoggingPipeline:
        steps = [
            ("Filtrar columnas definidas", FilterColumns(list(self.config.columns_keep))),
            (
                "Agregar id del user a las transacciones",
                FetchAndAttachId(
                    self.config.user_identifier_column,
                    self.config.user_entity,
                    self.config.join_how_user,
                    self.run_mode
                )
            ),
            (
                "Adjuntar id del RUC a las transacciones",
                FetchAndAttachId(
                    self.config.ruc_identifier_column,
                    self.config.ruc_entity,
                    self.config.join_how_ruc,
                    self.run_mode
                )
            ),
            (
                "Carga en DW", DWBatchedLoader(
                    db_alias=self.config.db_alias,
                    model_class=self.config.model_class,
                    mode=self.config.mode,
                    conflict_cols=list(self.config.conflict_cols),
                    batch_size=2500,
                    commit_per_batch=True
                )
            ),
        ]

        return LoggingPipeline(steps=steps, pipeline_name=self.config.pipeline_name)

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ejecuta el pipeline completo sobre el DataFrame de entrada.
        Retorno: DataFrame transformado (para seguir encadenando si se requiere).
        """
        return self._pipeline.fit_transform(df)
