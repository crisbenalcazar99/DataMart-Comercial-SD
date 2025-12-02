from dataclasses import dataclass
from typing import Sequence, Type, Optional

import pandas as pd

from datawarehouse.config.logging_pipeline import LoggingPipeline
from datawarehouse.etl.extract.db_extractor import DatabaseExtractor
from datawarehouse.etl.load.db_load_test import DWBatchedLoader
from datawarehouse.etl.transform.general_functions import TrimRowsObject, DropColumns
from datawarehouse.etl.transform.general_functions import FilterColumns, SortValues, DropDuplicatesTransform
from datawarehouse.models.Comercial.dim_clientes_entity import DimClientesEntity



@dataclass(frozen=True)
class VendedoresPipelineConfig:
    """
    Configuración declarativa del pipeline de RUCs.
    """
    # Columnas a conservar del DF original
    columns_keep: Sequence[str] = (
        "cod_cliente",
        "nom_cliente",
        "cif",
        'fecha_emision'
    )

    # Claves únicas para deduplicar
    unique_keys: Sequence[str] = ("cif",)

    # Configuración del ordenamiento
    sort_by: Sequence[str] = ("cif", "fecha_emision",)
    ascending: bool = False  # False = descendente

    # Configuración del loader
    db_alias: str = "QUANTA"
    model_class: Type = DimClientesEntity
    mode: str = "UPDATE"  # INSERT | IGNORE | UPDATE
    conflict_cols: tuple[str] = ("cif",)
    update_columns_conflict: tuple[str] = ('nom_cliente', 'update_date')

    # Nombre legible para logs
    pipeline_name: str = "Pipeline Clientes → DW"


class VendedoresPipeline:
    """
    Pipeline modular para preparar y cargar RUCs al DW.
    Pasos:
      1) Filtrado de columnas relevantes
      2) Ordenamiento por fecha de emisión
      3) Deduplicación por cif
      5) Eliminación de columnas auxiliares
      6) Carga a DW (INSERT/UPDATE/IGNORE según config)
    """

    def __init__(self, config: Optional[VendedoresPipelineConfig] = None) -> None:
        self.config = config or VendedoresPipelineConfig()
        self._pipeline = self._build_pipeline()

    def _build_pipeline(self) -> LoggingPipeline:
        steps = [
            ("Filtrar columnas definidas", FilterColumns(list(self.config.columns_keep))),
            ("Ordenar por fecha_emision", SortValues(list(self.config.sort_by), self.config.ascending)),
            ("Eliminar redundancias por RUC", DropDuplicatesTransform(list(self.config.unique_keys))),
            ("Eliminar columnas auxiliares", DropColumns(["fecha_emision"])),
            ("Carga en DW", DWBatchedLoader(
                db_alias=self.config.db_alias,
                model_class=self.config.model_class,
                mode=self.config.mode,
                conflict_cols=list(self.config.conflict_cols),
                batch_size=3000,
                commit_per_batch=True,
                update_cols=self.config.update_columns_conflict
            )),
        ]

        return LoggingPipeline(steps=steps, pipeline_name=self.config.pipeline_name)

    def run(self, df: pd.DataFrame) -> None:
        """
        Ejecuta el pipeline completo sobre el DataFrame de entrada.
        Retorno: None (la carga la realiza el DWLoader).
        """
        self._pipeline.fit_transform(df)

