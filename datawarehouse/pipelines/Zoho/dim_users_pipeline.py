from __future__ import annotations

from datawarehouse.etl.load.db_load_test import DWBatchedLoader
from datawarehouse.etl.transform.operatividad_functions import FetchAndAttachId, FetchAndAttachIdMultipleColumns
from datawarehouse.models.zoho.dim_localidades import DimLocalidadesEntity

from datawarehouse.models.zoho.dim_usuarios import DimUsuariosEntity

from dataclasses import dataclass, field
from typing import Dict, Optional, Sequence, Type, List, Literal

import pandas as pd

from datawarehouse.config.logging_pipeline import LoggingPipeline
from datawarehouse.etl.transform.general_functions import FilterColumns, DropDuplicatesTransform, RenameColumnsTransform
from datawarehouse.utils.RunMode import RunMode
from datawarehouse.utils.general_functions import load_sql_statement_operatividad


@dataclass(frozen=True)
class DatabasePipelineConfig:
    # Config del loader
    db_alias: str = "QUANTA"
    model_class: Type = DimUsuariosEntity
    mode: str = "UPDATE"  # INSERT | IGNORE | UPDATE
    conflict_cols: tuple[str] = ("cedula",)
    update_columns_conflict: tuple[str] = ("apellido_paterno", "apellido_materno", "nombre", "email",
                                           "phone", "fecha_nacimiento", "profesion", 'id_localidad', "update_date")

@dataclass(frozen=True)
class SourceSpec:
    name: str               # Alias humano: "subca1", "subca2", "camunda"
    db_alias: str           # "PORTAL" o "CAMUNDA"
    query_file: str         # Nombre del archivo .sql



@dataclass(frozen=True)
class UsersPipelineConfig:
    """
    Configuración declarativa del pipeline de usuarios.
    """
    columns_keep: Sequence[str] = ("cedula", "correo", "telefono", "nombre", "apellido_paterno", "apellido_materno",
                                   "profesion", "fecha_nacimiento", 'id_provincia', 'id_canton', 'id_parroquia', 'security_points')
    rename_map: Dict[str, str] = field(default_factory=lambda: {
        "correo": "email",
        "telefono": "phone",
    })

    unique_keys: Sequence[str] = ("cedula",)
    # Nombre legible para logs
    pipeline_name: str = "Pipeline Usuarios → DW"

    ## # Configuración de los transforms de enriquecimiento
    localidad_identifier_columns = ['id_provincia', 'id_canton', 'id_parroquia']
    localidad_entity = DimLocalidadesEntity
    join_how_localidad: Literal["left", "right", "inner", "outer", "cross"] = 'left'


class UsersPipeline:
    """
    Pipeline modular para preparar y cargar usuarios al DW.
    Pasos:
      1) Filtrado de columnas
      2) Deduplicación por claves únicas
      3) Renombrado de columnas
      4) Carga a DW (INSERT/UPDATE/IGNORE según config)
    """

    # Especificación de fuentes por modo
    SOURCES_BY_MODE: Dict[RunMode, List[SourceSpec]] = {
        RunMode.INICIAL: [
            SourceSpec("camunda", "CAMUNDA", "camunda.sql"),
        ],
    }

    def __init__(
            self,
            run_mode: RunMode = RunMode.INICIAL,
            dbconfig: Optional[DatabasePipelineConfig] = None,
            config: Optional[UsersPipelineConfig] = None) -> None:
        self.run_mode = run_mode
        self.config = config or UsersPipelineConfig()
        self.dbconfig = dbconfig or DatabasePipelineConfig()
        self._pipeline = self._build_pipeline()

    def _build_pipeline(self) -> LoggingPipeline:
        steps = [
            ("Filtrar columnas definidas", FilterColumns(list(self.config.columns_keep))),
            ("Eliminar redundancias", DropDuplicatesTransform(list(self.config.unique_keys))),
            ("Renombrar columnas", RenameColumnsTransform(dict_names=dict(self.config.rename_map))),
            (
                "Agregar id del user a las transacciones",
                FetchAndAttachIdMultipleColumns(
                    self.config.localidad_identifier_columns,
                    self.config.localidad_entity,
                    self.config.join_how_localidad,
                    self.run_mode
                )
            ),
            ("Carga en DW", DWBatchedLoader(
                db_alias=self.dbconfig.db_alias,
                model_class=self.dbconfig.model_class,
                mode=self.dbconfig.mode,
                conflict_cols=list(self.dbconfig.conflict_cols),
                batch_size=2000,
                commit_per_batch=True,
                update_cols=self.dbconfig.update_columns_conflict
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
