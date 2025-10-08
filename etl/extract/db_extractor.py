from sqlalchemy.exc import SQLAlchemyError
from common.connection import build_connection_url
from sklearn.base import BaseEstimator, TransformerMixin
from sqlalchemy import create_engine, text
import pandas as pd
from common.session_manager import get_session
import logging

class DatabaseExtractor(BaseEstimator, TransformerMixin):

    def __init__(self, db_alias, query=None, params=None):
        self.query = query
        self.db_alias = db_alias
        self.params = params
        self.log = logging.getLogger(__name__)

    def fit(self, X=None, y=None):
        return self

    def transform(self, X=None):
        if self.query is None:
            raise ValueError("Query must be provided for transformation.")

        try:

            stmt = text(self.query)
            with get_session(self.db_alias) as session:
                df = pd.read_sql(stmt, session.bind, params=self.params)
            return df
        except SQLAlchemyError as e:
            # Handle SQLAlchemy errors
            print(f"An error occurred while connecting to the database: {e}")
            return pd.DataFrame()

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return pd.DataFrame()
