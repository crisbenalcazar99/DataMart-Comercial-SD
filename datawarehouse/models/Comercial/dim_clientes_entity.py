from typing import Optional, Callable

import pandas as pd

from datawarehouse.models.base_model import BaseModel
from datawarehouse.models.base import Base
from sqlalchemy.orm import Session, Query
from sqlalchemy import Column, Integer, String, DateTime, func, Boolean
from sqlalchemy.orm import relationship


class DimClientesEntity(Base, BaseModel):
    __tablename__ = 'clientes'
    __table_args__ = {'schema': 'comercial_info'}
    id = Column(Integer, primary_key=True, autoincrement=True)
    estado_registro = Column(Boolean, default=True)
    cod_cliente = Column(String(30), nullable=False)
    nom_cliente = Column(String(255), nullable=False)
    cif = Column(String(15), nullable=False,  unique=True)
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
    transaccion = relationship("FactDetalleTransaccionesEntity", back_populates="cliente", cascade="all, delete-orphan")
    reasignacion = relationship("DimReasignacionesEntity", back_populates="cliente", cascade="all, delete-orphan")
    @classmethod
    def get_clientes(cls, session: Session, where_func=None):

        query = session.query(
            DimClientesEntity.id,
            DimClientesEntity.cif
        )

        if where_func:
            query = where_func(query)

        # 3. Ejecutar la query, obteniendo solo los datos filtrados
        rows = query.all()
        df_clientes = pd.DataFrame(rows, columns=['id_cliente', 'cif'])
        return df_clientes

    @classmethod
    def get_clientes_update(cls, session: Session, where_func=None):

        query = session.query(
            DimClientesEntity.cif,
            DimClientesEntity.update_date
        )

        if where_func:
            query = where_func(query)

        rows = query.all()
        df_clientes = pd.DataFrame(rows, columns=['cif', 'update_date'])
        return df_clientes

    @classmethod
    def get_cliente_id(cls, session: Session, where_func=None):
        query = session.query(DimClientesEntity.id)

        if where_func:
            query = where_func(query)

        return query.scalar()

    @classmethod
    def fetch_id_map(
            cls,
            session: Session,
            where_func: Optional[Callable[[Query], Query]] = None
    ):
        query = session.query(
            DimClientesEntity.id,
            DimClientesEntity.cif
        )

        if where_func:
            query = where_func(query)

        # 3. Ejecutar la query, obteniendo solo los datos filtrados
        rows = query.all()
        df_users = pd.DataFrame(rows, columns=['id_cliente', 'cif'])
        return df_users


