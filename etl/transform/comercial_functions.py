import sqlalchemy
from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
from common.session_manager import get_session
from models.Comercial.articulos_entity import ArticulosEntity
from models.Comercial.vendedores_entity import VendedoresEntity
from models.Comercial.clientes_entity import ClientesEntity
from models.Comercial.reasignaciones_entity import ReasignacionesEntity
from models.Comercial.clientes_entity import ClientesEntity


def request_vendedores_db(list_cod_vendedor):
    with get_session("LOCAL") as session:
        df_vendedores = VendedoresEntity.get_vendedores(
            session,
            where_func=lambda q: q.filter(
                VendedoresEntity.cod_vendedor.in_(list_cod_vendedor)
            )
        )
    return df_vendedores


class IdentificarVendedorComentario(BaseEstimator, TransformerMixin):
    def __init__(self, column):
        self.column = column

    def fit(self, X, y=None):
        # No fitting neccesary for this transformes
        return self

    def transform(self, X=pd.DataFrame()):
        X['cod_vendedor'] = X[self.column].str.extract(r'(\d{3})').astype('Int64')
        list_cod_vendedor = X['cod_vendedor'].dropna().unique().tolist()
        df_vendedores = request_vendedores_db(list_cod_vendedor)

        # 2. Hacer el merge
        X = X.merge(
            df_vendedores,
            how='left',
            on='cod_vendedor',
            suffixes=('', '_from_db')
        )

        # 3. Rellenar la columna
        X.loc[X['id_vendedor_from_db'].notna(), 'id_vendedor'] = X['id_vendedor_from_db']
        X.drop(columns=['cod_vendedor', 'comen3', 'id_vendedor_from_db'], inplace=True)
        X['id_vendedor'] = X['id_vendedor'].astype('Int64')
        return X


class IdentificarVendedorReasignaciones(BaseEstimator, TransformerMixin):
    def __init__(self, column_id_cliente, column_fecha_emision, column_id_vendedor):
        self.column_id_cliente = column_id_cliente
        self.column_fecha_emision = column_fecha_emision
        self.column_id_vendedor = column_id_vendedor

    def fit(self, X, y=None):
        # No fitting neccesary for this transformes
        return self

    def transform(self, X=pd.DataFrame()):
        X_temp = X.copy()
        X_temp = X_temp[X_temp[self.column_id_vendedor].isna()]
        with get_session("LOCAL") as session:
            for index, row in X_temp.iterrows():
                id_vendedor = ReasignacionesEntity.get_vendedor_id(
                    session=session,
                    where_func=lambda q: (q
                                          .filter(ReasignacionesEntity.id_cliente == row[self.column_id_cliente])
                                          .filter(ReasignacionesEntity.date_init <= row[self.column_fecha_emision])
                                          .filter(ReasignacionesEntity.date_end > row[self.column_fecha_emision])
                                          )

                )
                X.at[index, 'id_vendedor'] = id_vendedor
        X.drop(columns=['fecha_emision'], inplace=True)
        return X


class DeleteVendedorNoComercial(BaseEstimator, TransformerMixin):
    def __init__(self, column_id_vendedor):
        self.column_id_vendedor = column_id_vendedor

    def fit(self, X, y=None):
        # No fitting neccesary for this transformes
        return self

    def transform(self, X=pd.DataFrame()):
        id_vendedores_comercial = [414, 458, 483, 487, 519, 527, 936, 941, 946, 951, 956, 294]
        X[self.column_id_vendedor] = X[self.column_id_vendedor].where(
            X[self.column_id_vendedor].isin(id_vendedores_comercial),
            None
        )
        return X


class FacturaPagadaTransform(BaseEstimator, TransformerMixin):
    def __init__(self, column_name):
        self.column_name = column_name

    def fit(self, X, y=None):
        # No fitting neccesary for this transformes
        return self

    def transform(self, X=pd.DataFrame):
        X['factura_pagada'] = X[self.column_name] == 0
        return X


class CalculoSubtotalAbonado(BaseEstimator, TransformerMixin):
    def __init__(self, column_total_abonado, column_iva):
        self.column_total_abonado = column_total_abonado
        self.column_iva = column_iva

    def fit(self, X, y=None):
        # No fitting neccesary for this transformes
        return self

    def transform(self, X=pd.DataFrame):
        X['subtotal_abonado'] = X[self.column_total_abonado] / (1 + (X[self.column_iva] / 100))
        return X


class FetchClientIdTransform(BaseEstimator, TransformerMixin):
    def __init__(self, column_name):
        self.column_name = column_name

    def fit(self, X, y=None):
        return self

    def transform(self, X=pd.DataFrame):
        cif_list = X[self.column_name].unique().tolist()
        with get_session("LOCAL") as session:
            clients = ClientesEntity.get_clientes(
                session=session,
                where_func=lambda q: q.filter(
                    ClientesEntity.cif.in_(cif_list)
                )
            )
        X = X.merge(
            clients,
            how='left',
            on='cif'
        )
        return X


class FetchArticuloIdTransform(BaseEstimator, TransformerMixin):
    def __init__(self, column_name):
        self.column_name = column_name

    def fit(self, X, y=None):
        return self

    def transform(self, X=pd.DataFrame):
        cod_articulo_list = X[self.column_name].unique().tolist()
        with get_session("LOCAL") as session:
            articulos = ArticulosEntity.get_articulos(
                session=session,
                where_func=lambda q: q.filter(
                    ArticulosEntity.cod_articulo.in_(cod_articulo_list)
                )
            )
        X = X.merge(
            articulos,
            how='left',
            on='cod_articulo'
        )
        return X


class FetchVendedorIdTransform(BaseEstimator, TransformerMixin):
    def __init__(self, column_name):
        self.column_name = column_name

    def fit(self, X, y=None):
        return self

    def transform(self, X=pd.DataFrame):
        rucs_list = X[self.column_name].unique().tolist()
        with get_session("LOCAL") as session:
            clients = ClientesEntity.get_clientes(
                session=session,
                where_func=lambda q: q.filter(
                    ClientesEntity.cif.in_(rucs_list)
                )
            )
        X.merge(
            clients,
            how='left',
            on='ruc'
        )
        return X


class IdentifyChangeNewClients(BaseEstimator, TransformerMixin):
    def __init__(self, clients_tuple):
        self.clients_tuple = clients_tuple

    def fit(self, X, y=None):
        return self

    def transform(self, X=pd.DataFrame):
        with get_session('LOCAL') as session:
            clients_base = ClientesEntity.get_clientes_update(
                session=session,
                where_func=lambda q: q.filter(
                    ClientesEntity.cif.in_(self.clients_tuple)
                )
            )

        X = X.merge(
            clients_base.add_prefix('base_'),
            how='left',
            left_on='cif',  # Columna de X
            right_on='base_cif'  # Columna del clients_base
        )

        X = X[(X['base_update_date'].isna()) | (X['update_date'] > X['base_update_date'])]
        X.drop(['base_update_date', 'base_cif'], axis=1, inplace=True)

        return X


def build_date_filter_block(tx_date: str | None = None,
                            pay_date: str | None = None,
                            tuple_codart: tuple[str, ...] | None = None):
    clauses, params = [], {}
    if tx_date:
        clauses.append("f.emision > :date_replace_factura")
        params["date_replace_factura"] = tx_date
    if pay_date:
        clauses.append("tcxc.fecha_abono > :date_replace_abono")
        params["date_replace_abono"] = pay_date

    params['tuple_codart'] = tuple_codart
    filter_base = f"AND ({' OR '.join(clauses)})" if clauses else ""
    return filter_base, params


def apply_filter_to_query(query: str, tx_date: str | None, pay_date: str | None, tuple_codart: tuple[str, ...] | None):
    filter_base, params = build_date_filter_block(tx_date, pay_date, tuple_codart)
    return query.replace("{{filter_date_block}}", filter_base), params
