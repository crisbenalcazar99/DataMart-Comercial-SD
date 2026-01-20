import base64
import logging
import os

from typing import Union, Literal, Sequence, List

import numpy as np
from pandas import DataFrame
import pandas as pd
from sqlalchemy import tuple_

from datawarehouse.common.session_manager import get_session
from datawarehouse.models.Comercial.dim_articulos_entity import DimArticulosEntity
from datawarehouse.models.Comercial.dim_clientes_entity import DimClientesEntity
from datawarehouse.models.zoho.dim_rucs import DimRucsEntity
from datawarehouse.models.zoho.dim_usuarios import DimUsuariosEntity

from sklearn.base import BaseEstimator, TransformerMixin

from datawarehouse.utils.RunMode import RunMode

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad


class FetchUserIdTransform(BaseEstimator, TransformerMixin):
    def __init__(self, column_name):
        self.column_name = column_name

    def fit(self, X, y=None):
        return self

    def transform(self, X=pd.DataFrame):
        distinct_lookup_values = X[self.column_name].unique()
        with get_session("QUANTA") as session:
            users = DimUsuariosEntity.get_users(
                session=session,
                where_func=lambda q: q.filter(
                    DimUsuariosEntity.cedula.in_(distinct_lookup_values)
                )
            )
        X = X.merge(
            users,
            how='inner',
            on=self.column_name
        )
        X.drop(columns=[self.column_name], inplace=True)
        return X


class FetchRucIdTransform(BaseEstimator, TransformerMixin):
    def __init__(self, column_name, where_func=None):
        self.column_name = column_name
        self.where_func = where_func

    def fit(self, X, y=None):
        return self

    def transform(self, X=pd.DataFrame):
        with get_session("QUANTA") as session:
            df_rucs = DimRucsEntity.get_rucs(
                session=session,
                where_func=self.where_func
            )
        X = X.merge(
            df_rucs,
            how='inner',
            on=self.column_name
        )
        X.drop(columns=[self.column_name], inplace=True)
        return X


class FetchAndAttachId(BaseEstimator, TransformerMixin):
    def __init__(
            self,
            lookup_column: str,
            entity: Union[
                type(DimRucsEntity), type(DimUsuariosEntity), type(DimClientesEntity), type(DimArticulosEntity)],
            merge_how: Literal["left", "right", "inner", "outer", "cross"] = "inner",
            run_mode: RunMode = RunMode.INICIAL
    ):
        self.lookup_column = lookup_column
        self.entity = entity
        self.merge_how = merge_how
        self.run_mode = run_mode
        self.logger = logging.getLogger("DETELE LOGGER")

    def fit(self, X, y=None):
        return self

    def transform(self, X: DataFrame = None):
        distinct_lookup_values = X[self.lookup_column].dropna().unique()

        if self.run_mode is RunMode.INICIAL:
            where_func = None
        else:
            where_func = lambda q: q.filter(
                getattr(self.entity, self.lookup_column).in_(distinct_lookup_values)
            )

        self.logger.info(self.entity.__table__.name)
        with get_session("QUANTA") as session:
            df_fetch_ids = self.entity.fetch_id_map(
                session=session,
                where_func=where_func
            )
        X = X.merge(
            df_fetch_ids,
            how=self.merge_how,
            on=self.lookup_column
        )
        X.drop(columns=[self.lookup_column], inplace=True)
        return X


class FetchAndAttachIdMultipleColumns(BaseEstimator, TransformerMixin):
    def __init__(
            self,
            lookup_columns: Union[str, List[str]],
            entity: Union[
                type(DimRucsEntity), type(DimUsuariosEntity), type(DimClientesEntity), type(DimArticulosEntity)],
            merge_how: Literal["left", "right", "inner", "outer", "cross"] = "inner",
            run_mode: RunMode = RunMode.INICIAL
    ):
        """
        lookup_columns: puede ser un string (una sola columna)
                        o una lista de strings (varias columnas)
        """
        # Normalizamos internamente a lista
        if isinstance(lookup_columns, str):
            self.lookup_columns = [lookup_columns]
        else:
            self.lookup_columns = list(lookup_columns)
        self.entity = entity
        self.merge_how = merge_how
        self.run_mode = run_mode
        self.logger = logging.getLogger("FETCH ATTACH LOGGER")

    def fit(self, X, y=None):
        return self

    def transform(self, X: DataFrame = None) -> DataFrame:

        # 1) Obtenemos combinaciones distintas de las columnas de lookup
        #    (filtrando filas donde haya algún NaN en las columnas clave)
        distinct_lookup_df = (
            X[self.lookup_columns]
            .dropna(subset=self.lookup_columns, how="any")
            .drop_duplicates()
        )

        if self.run_mode is RunMode.INICIAL or distinct_lookup_df.empty:
            where_func = None
        else:
            # Pasamos las combinaciones como lista de tuplas
            lookup_tuples = [tuple(row) for row in distinct_lookup_df.to_numpy()]

            if len(self.lookup_columns) == 1:
                # Caso simple: una sola columna (mantiene compatibilidad)
                col_entity = getattr(self.entity, self.lookup_columns[0])
                where_func = lambda q: q.filter(col_entity.in_(
                    [t[0] for t in lookup_tuples]
                ))
            else:
                # Varios campos: usamos tuple_ de SQLAlchemy
                cols_entity = [getattr(self.entity, c) for c in self.lookup_columns]
                where_func = lambda q: q.filter(
                    tuple_(*cols_entity).in_(lookup_tuples)
                )

        self.logger.info(self.entity.__table__.name)

        with get_session("QUANTA") as session:
            # IMPORTANTE: aquí asumo que fetch_id_map soporta where_func
            # y devuelve un DataFrame que incluye las columnas de lookup
            # más la columna ID (o la que uses para mapear).
            df_fetch_ids = self.entity.fetch_id_map(
                session=session,
                where_func=where_func
            )

        # 2) Hacemos el merge por TODAS las columnas de lookup
        X = X.merge(
            df_fetch_ids,
            how=self.merge_how,
            on=self.lookup_columns
        )

        # 3) Eliminamos las columnas de lookup originales
        X.drop(columns=self.lookup_columns, inplace=True)

        X.info()

        return X


class SelectLatestByGroupTransform(BaseEstimator, TransformerMixin):
    """
    Mantiene el primer registro por grupo (id_user, ruc, producto)
    tras ordenar por fecha_caducidad en orden descendente.
    """

    def __init__(
            self,
            group_cols=('id_user', 'ruc', 'producto'),
            date_col='fecha_caducidad',
            reset_index=True,
            enforce_datetime=True
    ):
        self.group_cols = list(group_cols)
        self.date_col = date_col
        self.reset_index = reset_index
        self.enforce_datetime = enforce_datetime

    def fit(self, X, y=None):
        return self

    def transform(self, X: pd.DataFrame):
        # Validaciones mínimas
        required = set(self.group_cols + [self.date_col])
        missing = required - set(X.columns)
        if missing:
            raise ValueError(f"Faltan columnas requeridas: {sorted(missing)}")

        X_work = X.copy()

        # Ordena por grupos y fecha (desc) y toma el primero por grupo
        X_work = X_work.sort_values(self.group_cols + [self.date_col],
                                    ascending=[True] * len(self.group_cols) + [False],
                                    kind='mergesort')  # estable

        # Conserva el primer registro de cada grupo
        X_out = X_work.drop_duplicates(subset=self.group_cols, keep='first')

        if self.reset_index:
            X_out = X_out.reset_index(drop=True)

        return X_out


class LastPurchaseByUserProduct(BaseEstimator, TransformerMixin):
    def __init__(
            self,
            column_user: str,
            column_ruc: str,
            column_product: str,
            column_tipo_firma: str,
    ):
        self.column_user = column_user
        self.column_ruc = column_ruc
        self.column_product = column_product
        self.column_tipo_firma = column_tipo_firma

    def fit(self, X: DataFrame):
        return self

    def transform(self, X: DataFrame):
        kind_person_group = X[self.column_tipo_firma].dropna().unique()
        kind_product_group = X[self.column_product].dropna().unique()
        X_last_purchase = pd.DataFrame(columns=X.columns)
        frames = []

        for person in kind_person_group:
            for product in kind_product_group:
                mask = (
                        (X[self.column_tipo_firma] == person) & (X[self.column_product] == product)
                )

                X_filter = X.loc[mask].copy()
                if X_filter.empty:
                    continue

                if person == 'PN':
                    subset_columns = [self.column_user, self.column_product]
                else:
                    subset_columns = [self.column_user, self.column_ruc, self.column_product]

                X_filter = X_filter.drop_duplicates(subset=subset_columns, keep='first')

                frames.append(X_filter)

        if frames:
            X_last_purchase = pd.concat(frames, ignore_index=True)
        return X_last_purchase


class AddRucAuxiliar(BaseEstimator, TransformerMixin):
    def __init__(self, id_ruc_column, tipo_firma_column, id_ruc_auxiliar='id_ruc_auxiliar'):
        self.ruc_column: str = id_ruc_column
        self.tipo_firma_column: str = tipo_firma_column
        self.id_ruc_auxiliar = id_ruc_auxiliar

    def fit(self, X, y=None):
        return self

    def transform(self, X: DataFrame):
        X[self.id_ruc_auxiliar] = np.where(
            X[self.tipo_firma_column] != "PN",
            X[self.ruc_column],
            np.nan  # o np.nan si prefieres
        )
        return X


class GenerateLinkAutorenovacion(BaseEstimator, TransformerMixin):
    """
    Replace values in a column.
    :param X: Dataframe to be used to replace the values.
    :param column: Column to be replaced.
    :param old_value: List Value to be replaced.
    :param new_value: List New value.
    :return: Dataframe with the values replaced.
    """

    def __init__(self,
                 column_tipo_persona='tipo_firma',
                 column_id_firma='link_id_firma',
                 column_so='link_so',
                 column_emisor='link_emisor'):
        self.column_tipo_persona = column_tipo_persona
        self.column_id_firma = column_id_firma
        self.column_so = column_so
        self.column_emisor = column_emisor

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):


        # 1) Asignar link_tipo_persona según el tipo de persona
        conditions = [
            X[self.column_tipo_persona] == "PN",
            X[self.column_tipo_persona] == "RL",
            X[self.column_tipo_persona] == "ME"
        ]

        results = [
            "TIPO_PERSONA_SOL_NATURAL",
            "TIPO_PERSONA_SOL_LEGAL",
            "TIPO_PERSONA_SOL_EMPRESA"
        ]

        X["link_tipo_persona"] = np.select(conditions, results, default=None)

        # 2) Solo cifrar donde link_tipo_persona no sea nulo
        mask = X["link_tipo_persona"].notna()

        X.loc[mask, "link_renovacion"] = X.loc[mask].apply(
            lambda row: self.cifrar_texto_python(
                row[self.column_so],
                row[self.column_emisor],
                row["link_tipo_persona"],
                row[self.column_id_firma]
            ),
            axis=1
        )

        # Opcional: filas sin link tengan NaN explícito:
        X.loc[~mask, "link_renovacion"] = pd.NA

        return X

    @classmethod
    def cifrar_texto_python(cls, link_so, link_emisor, link_tipo_persona, link_id):
        texto = f'{{"so":"{link_so}","emisor":"{link_emisor}","tipoPersona":"{link_tipo_persona}","idFirma":"{link_id}","medioContacto":""}}'
        try:
            texto_bytes = texto.encode('utf-8')
            clave_original = "SecurityData2@2#Cifr#d0#ExtraSeguro12".encode('utf-8')
            clave_bytes = clave_original[:32]
            iv = os.urandom(16)
            cipher = AES.new(clave_bytes, AES.MODE_CBC, iv)
            texto_cifrado = cipher.encrypt(pad(texto_bytes, AES.block_size))
            resultado = base64.b64encode(texto_cifrado + b"::" + iv).decode('utf-8')
            return resultado
        except Exception as e:
            print("Error en cifrado:", str(e))
            return ''
