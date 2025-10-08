from sklearn.base import BaseEstimator, TransformerMixin
from common.session_manager import get_session
import pandas as pd
import logging
from config.logger_config import setup_logger


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

    def fit(self, X, y=None):
        return self

    def transform(self, X: pd.DataFrame):
        if not isinstance(X, pd.DataFrame):
            raise ValueError("El input de DWLoader debe ser un DataFrame de pandas.")

        try:
            with get_session(db_alias=self.db_alias) as session:
                self.model_class.persist_from_dataframe(session, X, self.conflict_cols, self.mode, self.update_cols)
            print(f"[INFO] {len(X)} filas cargadas a la tabla '{self.model_class.__table__.name}'.")
        except Exception as e:
            print(f"[ERROR] Falló la carga a PostgreSQL: {e}")
            raise
        print('Finalizo Proceso de Carga al DataWarehouse')
        return X
