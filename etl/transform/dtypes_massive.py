import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class DtypeDateTransform(BaseEstimator, TransformerMixin):
    """
    Transformer to convert date columns in a DataFrame to datetime format.
    """

    def __init__(self, columns, date_format='%Y-%m-%d %H:%M:%S'):
        self.date_format = date_format
        self.columns = columns if isinstance(columns, list) else [columns]

    def fit(self, X, y=None):
        # No fitting necessary for this transformer
        return self

    def transform(self, X):
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame.")

        for col in self.columns:
            X[col] = pd.to_datetime(X[col], errors='coerce', format=self.date_format)
            # Handle conversion errors by coercing invalid parsing to NaT

        return X


class DtypeCategoricalTransform(BaseEstimator, TransformerMixin):
    """
    Transformer to convert specified columns in a DataFrame to categorical type.
    """

    def __init__(self, columns):
        self.columns = columns if isinstance(columns, list) else [columns]

    def fit(self, X, y=None):
        # No fitting necessary for this transformer
        return self

    def transform(self, X):
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame.")

        for col in self.columns:
            X[col] = X[col].astype('category')

        return X


class DtypeFloatTransform(BaseEstimator, TransformerMixin):
    """
    Transformer to convert specified columns in a DataFrame to float type.
    """

    def __init__(self, columns):
        self.columns = columns if isinstance(columns, list) else [columns]

    def fit(self, X, y=None):
        # No fitting necessary for this transformer
        return self

    def transform(self, X):
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame.")
        for col in self.columns:
            X[col] = X[col].astype(float)

        return X


class DtypeIntegerTransform(BaseEstimator, TransformerMixin):
    """
    Transformer to convert specified columns in a DataFrame to integer type.
    """

    def __init__(self, columns, dtype_int='Int64'):
        self.columns = columns if isinstance(columns, list) else [columns]
        self.dtype_int = dtype_int

    def fit(self, X, y=None):
        # No fitting necessary for this transformer
        return self

    def transform(self, X):
        print('Inicio Proceso de Transformacion Enteros')
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame.")
        for col in self.columns:
            X[col] = X[col].astype(dtype=self.dtype_int)
        print('Finalizo Proceso de Transformacion Enteros')

        return X


class DtypeStringTransform(BaseEstimator, TransformerMixin):
    """
    Transformer to convert specified columns in a DataFrame to string type.
    """

    def __init__(self, columns):
        self.columns = columns if isinstance(columns, list) else [columns]

    def fit(self, X, y=None):
        # No fitting necessary for this transformer
        return self

    def transform(self, X):
        X.info()
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame.")

        for col in self.columns:
            X[col] = X[col].astype(str)

        return X