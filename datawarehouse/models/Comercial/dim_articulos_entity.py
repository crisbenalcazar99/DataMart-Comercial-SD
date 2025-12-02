from typing import Optional, Callable

import pandas as pd

from datawarehouse.models.base_model import BaseModel
from datawarehouse.models.base import Base
from sqlalchemy.orm import Session, Query
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, func, Boolean
from sqlalchemy.orm import relationship


class DimArticulosEntity(Base, BaseModel):
    __tablename__ = 'articulos'
    __table_args__ = {'schema': 'comercial_info'}
    id = Column(Integer, primary_key=True, autoincrement=True)
    estado_registro = Column(Boolean, default=True)
    cod_articulo = Column(String(30), nullable=False, unique=True)
    nom_articulo = Column(String(255), nullable=False)
    creation_date = Column(
        DateTime(timezone=False),
        server_default=func.now(),  # Fecha/hora de creación (la pone la BD)
        nullable=False
    )
    update_date = Column(
        DateTime(timezone=False),
        server_default=func.now(),  # Valor inicial al crear
        onupdate=func.now(),  # Se actualiza en cada UPDATE
        nullable=False
    )
    producto = Column(Integer, ForeignKey('comercial_info.catalogos.codigo_catalogo'), nullable=False)
    producto_relationship = relationship(
        "CatalogosEntity",
        foreign_keys=[producto],
        back_populates="articulo_producto"
    )
    tipo = Column(Integer, ForeignKey('comercial_info.catalogos.codigo_catalogo'), nullable=False)
    tipo_relationship = relationship(
        "CatalogosEntity",
        foreign_keys=[tipo],
        back_populates="articulo_tipo"
    )
    empresa_origen = Column(Integer, ForeignKey('comercial_info.catalogos.codigo_catalogo'), nullable=False)
    empresa_origen_relationship = relationship(
        'CatalogosEntity',
        foreign_keys=[empresa_origen],
        back_populates='articulo_empresa_origen'
    )

    transaccion = relationship("FactDetalleTransaccionesEntity", back_populates="articulo", cascade="all, delete-orphan")

    @classmethod
    def get_all_codigos_articulos(cls, session: Session):
        codigos_articulos = session.query(DimArticulosEntity.cod_articulo).all()
        return tuple(codigo[0] for codigo in codigos_articulos)

    @classmethod
    def get_list_codigos_articulos(cls, session: Session, where_func=None):
        query = session.query(
            DimArticulosEntity.cod_articulo
        )
        if where_func:
            query = where_func(query)

        # 3. Ejecutar la query, obteniendo solo los datos filtrados
        codigos_articulos = query.all()

        return tuple(codigo[0] for codigo in codigos_articulos)

    @classmethod
    def get_articulos(cls, session: Session, where_func=None):
        query = session.query(
            DimArticulosEntity.id,
            DimArticulosEntity.cod_articulo
        )

        if where_func:
            query = where_func(query)

        # 3. Ejecutar la query, obteniendo solo los datos filtrados
        rows = query.all()
        df_articulos = pd.DataFrame(rows, columns=['id_articulo', 'cod_articulo'])
        return df_articulos

    @classmethod
    def fetch_id_map(
            cls,
            session: Session,
            where_func: Optional[Callable[[Query], Query]] = None
    ):
        query = session.query(
            DimArticulosEntity.id,
            DimArticulosEntity.cod_articulo
        )

        if where_func:
            query = where_func(query)

        # 3. Ejecutar la query, obteniendo solo los datos filtrados
        rows = query.all()
        df_users = pd.DataFrame(rows, columns=['id_articulo', 'cod_articulo'])
        return df_users
