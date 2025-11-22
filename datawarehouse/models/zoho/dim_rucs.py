from typing import Optional, Callable

import pandas as pd
from sqlalchemy.orm import Session, Query, relationship

from datawarehouse.models.base_model import BaseModel
from datawarehouse.models.base import Base
from sqlalchemy import Column, String, DateTime, func, BigInteger, Boolean


class DimRucsEntity(Base, BaseModel):
    __tablename__ = 'dim_rucs'
    __table_args__ = {'schema': 'operatividad'}
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ruc = Column(String(32), unique=True, nullable=False)
    razon_social = Column(String(255))
    actividad_ruc = Column(String(255))
    clase_contribuyente = Column(String(255))
    sector_economico = Column(String(255))
    contribuyente_fantasma = Column(Boolean)
    is_company = Column(Boolean)
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

    @classmethod
    def fetch_id_map(
            cls,
            session: Session,
            where_func: Optional[Callable[[Query], Query]] = None
    ):
        query = session.query(
            DimRucsEntity.id,
            DimRucsEntity.ruc
        )

        if where_func:
            query = where_func(query)

        # Ejecutamos la quiery obtenida para los datos filtrados
        rows = query.all()
        df_rucs = pd.DataFrame(rows, columns=['id_ruc', 'ruc'])
        return df_rucs

    @classmethod
    def get_rucs(cls, session: Session, where_func=None):
        query = session.query(
            DimRucsEntity.id,
            DimRucsEntity.ruc
        )

        if where_func:
            query = where_func(query)

        # Ejecutamos la quiery obtenida para los datos filtrados
        rows = query.all()
        df_rucs = pd.DataFrame(rows, columns=['id_ruc', 'ruc'])
        return df_rucs

    transaction_history = relationship('FactTransaccionesOperatividadEntity', back_populates="rucs", cascade="all, delete-orphan")
    current_products = relationship('FactServiciosActivosEntity', back_populates='rucs', cascade='all, delete-orphan')
