from __future__ import annotations


from datawarehouse.etl.load.db_load_test import DWBatchedLoader

from datawarehouse.models.zoho.dim_usuarios import DimUsuariosEntity

from dataclasses import dataclass, field
from typing import Dict, Optional, Sequence, Type

import pandas as pd

from datawarehouse.config.logging_pipeline import LoggingPipeline
from datawarehouse.etl.transform.general_functions import FilterColumns, DropDuplicatesTransform, RenameColumnsTransform


@dataclass(frozen=True)
class UsersPipelineConfig:
    """
    Configuración declarativa del pipeline de usuarios.
    """
    columns_keep: Sequence[str] = ("cedula", "correo", "telefono", "nombre", "apellido_paterno", "apellido_materno",
                                   "profesion", "fecha_nacimiento")
    rename_map: Dict[str, str] = field(default_factory=lambda: {
        "correo": "email",
        "telefono": "phone",
    })

    unique_keys: Sequence[str] = ("cedula",)

    # Config del loader
    db_alias: str = "QUANTA"
    model_class: Type = DimUsuariosEntity
    mode: str = "IGNORE"  # INSERT | IGNORE | UPDATE
    conflict_cols: tuple[str] = ("cedula", )

    # Nombre legible para logs
    pipeline_name: str = "Pipeline Usuarios → DW"


class UsersPipeline:
    """
    Pipeline modular para preparar y cargar usuarios al DW.
    Pasos:
      1) Filtrado de columnas
      2) Deduplicación por claves únicas
      3) Renombrado de columnas
      4) Carga a DW (INSERT/UPDATE/IGNORE según config)
    """

    def __init__(self, config: Optional[UsersPipelineConfig] = None) -> None:
        self.config = config or UsersPipelineConfig()
        self._pipeline = self._build_pipeline()

    def _build_pipeline(self) -> LoggingPipeline:
        steps = [
            ("Filtrar columnas definidas", FilterColumns(list(self.config.columns_keep))),
            ("Eliminar redundancias", DropDuplicatesTransform(list(self.config.unique_keys))),
            ("Renombrar columnas", RenameColumnsTransform(dict_names=dict(self.config.rename_map))),
            ("Carga en DW", DWBatchedLoader(
                db_alias=self.config.db_alias,
                model_class=self.config.model_class,
                mode=self.config.mode,
                conflict_cols=list(self.config.conflict_cols),
                batch_size=3000,
                commit_per_batch=True
            )),
        ]
        return LoggingPipeline(steps=steps, pipeline_name=self.config.pipeline_name)

    def run(self, df: pd.DataFrame) -> None:
        """
        Ejecuta el pipeline completo sobre el DataFrame de entrada.
        Retorno: None (la carga la realiza el DWLoader).
        """
        self._pipeline.fit_transform(df)


# -------------------------
# Uso básico
# -------------------------
# df_general = ...  # tu DataFrame consolidado
# UsersPipeline().run(df_general)

# -------------------------
# Uso avanzado (custom)
# -------------------------
# cfg = UsersPipelineConfig(
#     columns_keep=("cedula", "correo", "telefono", "nombre", "extra"),
#     rename_map={"nombre": "name", "correo": "email", "telefono": "phone"},
#     unique_keys=("cedula",),
#     db_alias="QUANTA",
#     model_class=UsersEntity,
#     mode="UPDATE",
#     conflict_cols=("cedula",),
#     pipeline_name="Usuarios (Custom) → DW",
# )
# UsersPipeline(cfg).run(df_general)
