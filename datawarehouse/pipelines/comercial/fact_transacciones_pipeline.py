from dataclasses import dataclass
from typing import Type, Optional, Literal


import pandas as pd
from sqlalchemy import and_, Sequence


from datawarehouse.config.logging_pipeline import LoggingPipeline
from datawarehouse.etl.load.db_load_test import DWBatchedLoader
from datawarehouse.etl.transform.general_functions import FilterColumns
from datawarehouse.etl.transform.operatividad_functions import FetchAndAttachId
from datawarehouse.models.Comercial.dim_articulos_entity import DimArticulosEntity
from datawarehouse.models.Comercial.dim_clientes_entity import DimClientesEntity
from datawarehouse.models.Comercial.fact_detalle_transacciones_entity import FactDetalleTransaccionesEntity
from datawarehouse.models.Comercial.fact_facturas_entity import FactFacturasEntity
from datawarehouse.utils.RunMode import RunMode


@dataclass(frozen=True)
class TransactionDetailsPipelineConfig:
    """
    Configuración declarativa del pipeline de histórico de transacciones.
    """
    # Columnas a conservar del DF original
    columns_keep: Sequence[str] = (
        'numfac',
        'cif',
        'cod_articulo',
        'id_vendedor',
        'subtotal_articulo',
        'cantidad_articulo',
        'id_transaction',
        'tipo_vendedor'
    )

    # Configuración de los transforms de enriquecimiento
    cliente_identifier_column: str = "cif"
    cliente_entity: Type = DimClientesEntity
    join_how_cliente: Literal["left", "right", "inner", "outer", "cross"] = "inner"

    articulo_identifier_column: str = "cod_articulo"
    articulo_entity: Type = DimArticulosEntity
    join_how_articulo: Literal["left", "right", "inner", "outer", "cross"] = "inner"

    factura_identifier_column: str = "numfac"
    factura_entity: Type = FactFacturasEntity
    join_how_factura: Literal["left", "right", "inner", "outer", "cross"] = "inner"

    # Configuración del loader
    db_alias: str = "QUANTA"
    model_class: Type = FactDetalleTransaccionesEntity
    mode: str = "UPDATE"  # INSERT | IGNORE | UPDATE
    conflict_cols: tuple[str] = ("id_transaction",)
    update_columns_conflict: tuple[str] = ('update_date', 'id_vendedor')

    # Nombre legible para logs
    pipeline_name: str = "Pipeline Detalle Transacciones Comercial  → DW"


class TransactionDetailsPipeline:
    """
    Pipeline modular para preparar y cargar el histórico de transacciones al DW.
    Pasos:
      1) Filtrado de columnas relevantes
      2) Enriquecer con id de usuario
      3) Enriquecer con id de RUC
      4) Carga a DW (INSERT/UPDATE/IGNORE según config)
    """

    def __init__(self, run_mode: RunMode = RunMode.INICIAL,
                 config: Optional[TransactionDetailsPipelineConfig] = None, ) -> None:
        self.run_mode = run_mode
        self.config = config or TransactionDetailsPipelineConfig()
        self._pipeline = self._build_pipeline()

    def _build_pipeline(self) -> LoggingPipeline:
        steps = [
            ("Filtrar columnas definidas", FilterColumns(list(self.config.columns_keep))),
            (
                "Agregar id del Articulos a las transacciones",
                FetchAndAttachId(
                    self.config.articulo_identifier_column,
                    self.config.articulo_entity,
                    self.config.join_how_articulo,
                    self.run_mode
                )
            ),
            (
                "Adjuntar id del Clientes a las transacciones",
                FetchAndAttachId(
                    self.config.cliente_identifier_column,
                    self.config.cliente_entity,
                    self.config.join_how_cliente,
                    self.run_mode
                )
            ),

            (
                "Adjuntar id del Facturas a las transacciones",
                FetchAndAttachId(
                    self.config.factura_identifier_column,
                    self.config.factura_entity,
                    self.config.join_how_factura,
                    self.run_mode
                )
            ),
            (
                "Carga en DW", DWBatchedLoader(
                    db_alias=self.config.db_alias,
                    model_class=self.config.model_class,
                    mode=self.config.mode,
                    conflict_cols=list(self.config.conflict_cols),
                    batch_size=500,
                    commit_per_batch=True,
                    update_cols=self.config.update_columns_conflict
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
