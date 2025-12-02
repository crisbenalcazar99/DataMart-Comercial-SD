from typing import Optional, Callable

import pandas as pd

from datawarehouse.models.base_model import BaseModel
from datawarehouse.models.base import Base
from sqlalchemy.orm import Session, Query
from sqlalchemy import Column, Integer, String, DateTime, Numeric, Boolean, func
from sqlalchemy.orm import relationship


class FactFacturasEntity(Base, BaseModel):
    __tablename__ = 'facturas'
    __table_args__ = {'schema': 'comercial_info'}
    id = Column(Integer, primary_key=True, autoincrement=True)
    estado_registro = Column(Boolean, default=True)
    numfac = Column(String(15), nullable=False, unique=True)
    numdoc = Column(String(15))
    comen2 = Column(String(255))
    comen3 = Column(String(255))
    numero_factura = Column(String(32))
    subtotal = Column(Numeric(14, 4))
    total = Column(Numeric(14, 4))
    iva = Column(Numeric(14, 4))
    total_retencion = Column(Numeric(14, 4))
    total_abonado = Column(Numeric(14, 4))
    subtotal_abonado = Column(Numeric(14, 4))
    saldo_factura = Column(Numeric(14, 4))
    factura_pagada = Column(Boolean)
    fecha_emision = Column(DateTime, nullable=True)
    fecha_abono = Column(DateTime, nullable=True)
    estado_factura = Column(String(32))
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



    # Relación inversa (uno a muchos)
    transaccion = relationship("FactDetalleTransaccionesEntity", back_populates="factura", cascade="all, delete-orphan")


    @classmethod
    def get_last_transaction_date(cls, session: Session, where_func=None):
        query = session.query(
            func.max(FactFacturasEntity.fecha_emision)
        )
        if where_func:
            query = where_func(query)

        result = query.scalar()
        return str(result) if result else None

    @classmethod
    def get_last_payment_date(cls, session: Session, where_func=None):
        query = session.query(
            func.max(FactFacturasEntity.fecha_abono)
        )
        if where_func:
            query = where_func(query)

        result = query.scalar()
        return str(result) if result else None

    @classmethod
    def fetch_id_map(
            cls,
            session: Session,
            where_func: Optional[Callable[[Query], Query]] = None
    ):
        query = session.query(
            FactFacturasEntity.id,
            FactFacturasEntity.numfac
        )

        if where_func:
            query = where_func(query)

        # 3. Ejecutar la query, obteniendo solo los datos filtrados
        rows = query.all()
        df_users = pd.DataFrame(rows, columns=['id_factura', 'numfac'])
        return df_users

