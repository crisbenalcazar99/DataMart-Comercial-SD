import pandas as pd

from models.base_model import BaseModel
from models.base import Base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, DateTime, Numeric, Boolean, func
from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey


class FacturasEntity(Base, BaseModel):
    __tablename__ = 'facturas'
    id = Column(Integer, primary_key=True, autoincrement=True)
    numfac = Column(String(15), nullable=False, unique=True)
    numdoc = Column(String(15), nullable=False)
    comen2 = Column(String(255), nullable=False, unique=False)
    comen3 = Column(String(255), nullable=False, unique=False)
    numero_factura = Column(String(30), nullable=False, unique=False)
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

    # Relación inversa (uno a muchos)
    transaccion = relationship("TransaccionesEntity", back_populates="factura", cascade="all, delete-orphan")


    @classmethod
    def get_last_transaction_date(cls, session: Session, where_func=None):
        query = session.query(
            func.max(FacturasEntity.fecha_emision)
        )
        if where_func:
            query = where_func(query)

        result = query.scalar()
        return str(result) if result else None

    @classmethod
    def get_last_payment_date(cls, session: Session, where_func=None):
        query = session.query(
            func.max(FacturasEntity.fecha_abono)
        )
        if where_func:
            query = where_func(query)

        result = query.scalar()
        return str(result) if result else None




