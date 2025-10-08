from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd


class RenameColumnsTransform(BaseEstimator, TransformerMixin):
    def __init__(self, dict_names):
        self.dict_names = dict_names

    def fit(self, X, y=None):
        # No fitting neccesary for this transformes
        return self

    def transform(self, X=pd.DataFrame()):
        X.rename(columns=self.dict_names, inplace=True)
        return X


class DropDuplicatesTransform(BaseEstimator, TransformerMixin):
    def __init__(self, subset, keep='first', reset_index=True, drop_index=True):
        self.subset = subset
        self.keep = keep
        self.reset_index = reset_index
        self.drop_index = drop_index

    def fit(self, X, y=None):
        return self

    def transform(self, X=pd.DataFrame):
        X.drop_duplicates(subset=self.subset, keep=self.keep, inplace=True)
        if self.reset_index:
            X.reset_index(drop=self.drop_index, inplace=True)
        return X


class UpperLetterTransform(BaseEstimator, TransformerMixin):
    def __init__(self, columns):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X=pd.DataFrame):
        for column in self.columns:
            if column in X.columns:
                X[column] = X[column].str.upper()
        return X


class ReplaceTextTransform(BaseEstimator, TransformerMixin):
    def __init__(self, old_value, new_value, column):
        self.old_value = old_value
        self.new_value = new_value
        self.column = column

    def fit(self, X, y=None):
        return self

    def transform(self, X=pd.DataFrame):
        X[self.column] = X[self.column].str.replace(self.old_value, self.new_value)
        return X


class TrimRowsObject(BaseEstimator, TransformerMixin):
    def __init__(self, columns=None):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X=pd.DataFrame):
        if self.columns is None:
            self.columns = [column for column in X.select_dtypes(include=['object', 'string'])]
        for column in self.columns:
            X[column] = X[column].str.strip()
        return X


class SortValues(BaseEstimator, TransformerMixin):
    def __init__(self, columns, order_ascending=True):
        self.columns = columns
        self.order_ascending = order_ascending

    def fit(self, X, y=None):
        return self

    def transform(self, X=pd.DataFrame):
        X.sort_values(by=self.columns, ascending=self.order_ascending, inplace=True)
        return X


class DropColumns(BaseEstimator, TransformerMixin):
    def __init__(self, columns):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X=pd.DataFrame()):
        X.drop(columns=self.columns, inplace=True)
        return X
