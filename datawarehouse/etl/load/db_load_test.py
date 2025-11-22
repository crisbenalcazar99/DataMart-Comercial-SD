import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import logging

from datawarehouse.common.session_manager import get_session


class DWLoader(BaseEstimator, TransformerMixin):
    def __init__(self, db_alias, model_class, conflict_cols=None, mode="INSERT", update_cols=None):
        """
        Carga los datos al data warehouse (PostgreSQL por defecto).
        - table_name: nombre de la tabla destino
        - if_exists: comportamiento si la tabla existe ('fail', 'replace', 'append')
        - index: si incluye índice del DataFrame en la tabla
        """
        self.model_class = model_class
        self.db_alias = db_alias
        self.conflict_cols = conflict_cols
        self.mode = mode
        self.update_cols = update_cols
        self.logger = logging.getLogger("database logger")

    def fit(self, X, y=None):
        return self

    def transform(self, X: pd.DataFrame):
        if not isinstance(X, pd.DataFrame):
            raise ValueError("El input de DWLoader debe ser un DataFrame de pandas.")

        try:
            # EL Bulk de Update Date no es capaz de reconocer Valores NaT, np.nan mapea ese valor y lo tranformea a
            # None que es Similar a Null. El pd.notnull no tuvo esa capacidad, dejaba el NaT de igual manera.
            X = X.replace({np.nan: None})
            with get_session(db_alias=self.db_alias) as session:
                self.model_class.persist_from_dataframe(session, X, self.conflict_cols, self.mode, self.update_cols)
            print(f"[INFO] {len(X)} filas cargadas a la tabla '{self.model_class.__table__.name}'.")
        except Exception as e:
            self.logger.error("Error en inserción SQLAlchemy: %s", e.__class__.__name__)
            self.logger.error("Mensaje: %s", e)
            raise
        print('Finalizo Proceso de Carga al DataWarehouse')
        return X


class DWBatchedLoader(BaseEstimator, TransformerMixin):
    def __init__(
            self,
            db_alias,
            model_class,
            conflict_cols=None,
            mode: str = "INSERT",
            update_cols=None,
            batch_size: int = 10_000,
            commit_per_batch: bool = True,
    ):
        """
        Carga los datos al Data Warehouse en batches.

        Parámetros:
        - db_alias: alias de la base de datos (para get_session)
        - model_class: clase de SQLAlchemy que expone persist_from_dataframe(session, df, conflict_cols, mode, update_cols)
        - conflict_cols: columnas de conflicto para ON CONFLICT
        - mode: modo de operación ('INSERT', 'UPSERT', etc. según maneje tu persist_from_dataframe)
        - update_cols: columnas a actualizar en caso de conflicto (para UPSERT, etc.)
        - batch_size: tamaño de cada batch (número de filas del DataFrame)
        - commit_per_batch: si True, hace commit después de cada batch
        """
        self.model_class = model_class
        self.db_alias = db_alias
        self.conflict_cols = conflict_cols
        self.mode = mode
        self.update_cols = update_cols
        self.batch_size = batch_size
        self.commit_per_batch = commit_per_batch
        self.logger = logging.getLogger("database logger")

    def fit(self, X, y=None):
        return self

    def _iter_batches(self, X: pd.DataFrame):
        """
        Generador de batches del DataFrame.
        """
        n_rows = len(X)
        for start in range(0, n_rows, self.batch_size):
            end = start + self.batch_size
            yield start, end, X.iloc[start:end]

    def transform(self, X: pd.DataFrame):
        if not isinstance(X, pd.DataFrame):
            raise ValueError("El input de DWBatchedLoader debe ser un DataFrame de pandas.")

        if X.empty:
            self.logger.info("DataFrame vacío recibido en DWBatchedLoader. No se realiza ninguna carga.")
            print("No hay filas para cargar al DataWarehouse.")
            return X

        try:
            # Mismo tratamiento de NaN / NaT que eb DWLoader original
            X = X.replace({np.nan: None})

            total_rows = len(X)
            total_batches = (total_rows + self.batch_size - 1) // self.batch_size
            self.logger.info(
                "Iniciando carga en batches. Total filas: %s, batch_size: %s, total_batches: %s",
                total_rows,
                self.batch_size,
                total_batches,
            )

            with get_session(db_alias=self.db_alias) as session:
                for i, (start, end, batch_df) in enumerate(self._iter_batches(X), start=1):
                    try:
                        self.model_class.persist_from_dataframe(
                            session,
                            batch_df,
                            self.conflict_cols,
                            self.mode,
                            self.update_cols,
                        )
                        if self.commit_per_batch:
                            session.commit()

                        msg = (
                            f"[INFO] Batch {i}/{total_batches}: "
                            f"{len(batch_df)} filas cargadas "
                            f"a la tabla '{self.model_class.__table__.name}' "
                            f"(filas {start}–{end - 1})."
                        )
                        self.logger.info(msg)
                    except Exception as e_batch:
                        # Si un batch falla, logueamos el contexto del batch
                        self.logger.error(
                            "Error en inserción del batch %s/%s. Rango de filas: %s-%s. Error: %s - %s",
                            i,
                            total_batches,
                            start,
                            end - 1,
                            e_batch.__class__.__name__,
                            e_batch,
                        )
                        # Importante dejar que la excepción suba para que el pipeline falle de forma explícita
                        raise

        except Exception as e:
            self.logger.error("Error en inserción SQLAlchemy (proceso por batches): %s", e.__class__.__name__)
            self.logger.error("Mensaje: %s", e)
            raise

        print("Finalizó Proceso de Carga al DataWarehouse (por batches).")
        return X
