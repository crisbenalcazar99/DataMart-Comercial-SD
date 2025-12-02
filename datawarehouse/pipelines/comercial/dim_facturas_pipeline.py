from dataclasses import field, dataclass
from typing import Sequence, Dict, Optional, Type

import pandas as pd

from datawarehouse.config.logging_pipeline import LoggingPipeline
from datawarehouse.etl.load.db_load_test import DWBatchedLoader
from datawarehouse.etl.transform.general_functions import FilterColumns, SortValues, DropDuplicatesTransform
from datawarehouse.models.Comercial.fact_facturas_entity import FactFacturasEntity


@dataclass(frozen=True)
class DatabaseLoaderPipelineConfig:
    # Config del loader
    db_alias: str = "QUANTA"
    model_class: Type = FactFacturasEntity
    mode: str = "UPDATE"  # Supuesto razonable; ajusta a INSERT/IGNORE si lo necesitas

    # Conflic Columns para Loader DW
    conflict_cols: tuple[str] = ("numfac",)


@dataclass(frozen=True)
class FacturasComercialPipelineConfig:
    """
    Configuración declarativa del pipeline de servicios/productos activos.
    """
    # Columnas a conservar del DF original
    COLUMNS_KEEP: Sequence[str] = (
        'numfac',
        'numdoc',
        'comen2',
        'comen3',
        'numero_factura',
        'subtotal',
        'total',
        'iva',
        'total_retencion',
        'total_abonado',
        'saldo_factura',
        'fecha_emision',
        'fecha_abono',
        'estado_factura',

    )

    # Ordenamiento para seleccionar el registro 'actual'
    sort_by: Sequence[str] = ("numfac", "fecha_emision")
    ascending: bool = False  # False = descendente

    # Claves únicas para deduplicar
    unique_keys: Sequence[str] = ("numfac",)
    update_columns_confict: tuple[str] = ('total_retencion', 'total_abonado', 'saldo_factura', 'subtotal_abonado',
                                          'fecha_abono', 'update_date', 'factura_pagada')

    # Renombrado de columnas
    rename_map: Dict[str, str] = field(default_factory=lambda: {
        "fecha_caducidad": "fecha_caducidad_max",
    })

    # Nombre legible para logs
    pipeline_name: str = "Pipeline Facturas Comercial → DW"


class FacturasComercialPipeline:
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
            config: Optional[FacturasComercialPipelineConfig] = None,
            config_database: Optional[DatabaseLoaderPipelineConfig] = None
    ) -> None:
        self.config = config or FacturasComercialPipelineConfig()
        self.config_database = config_database or DatabaseLoaderPipelineConfig()
        self._pipeline_transform = self._build_transform_pipeline()

    def _build_transform_pipeline(self) -> LoggingPipeline:
        steps = [
            ("Filtrar columnas definidas", FilterColumns(list(self.config.COLUMNS_KEEP))),
            (f"Sort By specify columns: {self.config.sort_by}",
             SortValues(list(self.config.sort_by), self.config.ascending)),
            ("Eliminar redundancias por numfac", DropDuplicatesTransform(list(self.config.unique_keys))),
            ("Carga en DW", DWBatchedLoader(
                db_alias=self.config_database.db_alias,
                model_class=self.config_database.model_class,
                mode=self.config_database.mode,
                conflict_cols=list(self.config_database.conflict_cols),
                batch_size=500,
                commit_per_batch=True,
                update_cols=self.config.update_columns_confict
            ))
        ]

        return LoggingPipeline(steps=steps, pipeline_name=self.config.pipeline_name)

    def run(self, df: pd.DataFrame) -> None:
        """
        Ejecuta el pipeline completo sobre el DataFrame de entrada.
        Retorno: DataFrame transformado (por si se quiere seguir encadenando).
        """
        self._pipeline_transform.fit_transform(df)
        return None
