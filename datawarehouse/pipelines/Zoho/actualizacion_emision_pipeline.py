from dataclasses import dataclass
from typing import Type, Optional, Literal

from sqlalchemy.dialects import postgresql

from datawarehouse.common.session_manager import get_session
from datawarehouse.config.logging_pipeline import LoggingPipeline
from datawarehouse.etl.extract.db_extractor import DatabaseExtractor
from datawarehouse.etl.load.db_load_test import DWBatchedLoader, DWLoader, DWBatchedUpdater
from datawarehouse.etl.transform.dtypes_massive import DtypeDateTransform, DtypeIntegerTransform
from datawarehouse.etl.transform.general_functions import DropRowNaDates
from datawarehouse.models.zoho.fact_servicios_activos import FactServiciosActivosEntity
from datawarehouse.models.zoho.fact_transacciones_operatividad import FactTransaccionesOperatividadEntity
from datawarehouse.utils.general_functions import load_sql_statement_comercial, load_sql_statement_operatividad

@dataclass(frozen=True)
class DatabasePipelineConfig:
    # Config del loader
    db_alias: str = "QUANTA"
    model_class: type[FactTransaccionesOperatividadEntity] = FactTransaccionesOperatividadEntity
    mode: str = "UPDATE"
    conflict_cols: tuple[str] = ("serial_firma",)
    update_columns_conflict: tuple[str] = ('update_date', 'fecha_emision', 'fecha_caducidad')
    pipeline_name: str = "Pipeline Update Emision Historico Productos → DW"

@dataclass(frozen=True)
class DatabasePipelineConfigCurrent:
    # Config del loader
    db_alias: str = "QUANTA"
    model_class: type[FactServiciosActivosEntity] = FactServiciosActivosEntity
    mode: str = "UPDATE"
    conflict_cols: tuple[str] = ("serial_firma",)
    update_columns_conflict: tuple[str] = ('update_date', 'fecha_emision', 'fecha_caducidad_max')
    pipeline_name: str = "Pipeline Update Emision Vigente Productos → DW"


@dataclass
class UpdateEmisionConfig:
    """
    Configuración declarativa del pipeline de Actualizacion de Emision Pipeline.
    """
    query_file: str = "update_emision.sql"
    db_alias_source: Literal["CAMUNDA"] = "CAMUNDA"


@dataclass(frozen=True)
class SourceSpec:
    query_file: str = ""


class UpdateTrasactionHistoryPipeline:
    COLUMNS_DATETIME = [
        'fecha_caducidad',
        'fecha_emision',
    ]
    COLUMNS_INTEGER = [
        'id_tramite',
    ]
    """
    Pipeline modular para actualizar la fecha de emision y caducidad de las  transacciones al DW.
    Pasos:
      1) Busca las firmas emitidas de camunda sin una fecha de emision
      2) Busca si esos registros cuenta con una fecha de emision
      3) Carga a DW (INSERT/UPDATE/IGNORE según config)
    """

    def __init__(
            self,
            dbconfig: Optional[DatabasePipelineConfig] = None,
            dbconfig_current: Optional[DatabasePipelineConfigCurrent] = None,
            config: Optional[UpdateEmisionConfig] = None
    ) -> None:
        self.dbconfig = dbconfig or DatabasePipelineConfig()
        self.dbconfig_current = dbconfig_current or DatabasePipelineConfigCurrent()
        self.config = config or UpdateEmisionConfig()

    def run(self):
        folder = 'actualizacion_incremental'
        # COnstruir y ejecutar el Pipeline
        params = self._build_params_for_mode()
        query = load_sql_statement_operatividad(folder, self.config.query_file)

        pipe = self._build_pipeline(query, params)
        pipe.fit_transform(None)

    def _build_pipeline(self, query, params) -> LoggingPipeline:
        return LoggingPipeline(
            steps=[
                ("extractor database",
                 DatabaseExtractor(db_alias=self.config.db_alias_source, query=query, params=params)),
                ("Transform Datetime", DtypeDateTransform(self.COLUMNS_DATETIME)),
                ("Transform Integer", DtypeIntegerTransform(self.COLUMNS_INTEGER)),
                ("Eliminar registros sin Fecha Emision", DropRowNaDates(['fecha_caducidad'])),
                # (
                #     "Carga en DW", DWBatchedUpdater(
                #         db_alias=self.dbconfig.db_alias,
                #         model_class=self.dbconfig.model_class,
                #         update_date_col="fecha_caducidad"
                #     )
                # ),
                (
                    "Carga en DW", DWBatchedUpdater(
                        db_alias=self.dbconfig_current.db_alias,
                        model_class=self.dbconfig_current.model_class,
                        update_date_col="fecha_caducidad_max"
                    )
                ),

            ],
            pipeline_name=f"Pipeline All {self.dbconfig.pipeline_name}"

        )

    # --------------------------
    # Helpers de construcción
    # --------------------------

    def _build_params_for_mode(self) -> dict[str, any]:
        with get_session(self.dbconfig_current.db_alias) as session:
            params = self.dbconfig_current.model_class.get_register_idtramite(
                session=session,
                where_func=lambda q: q.filter(
                    self.dbconfig_current.model_class.fecha_caducidad_max.is_(None)
                )
            )
        return {"ids": params}
