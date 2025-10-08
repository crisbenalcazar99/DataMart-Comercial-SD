import pandas as pd

from models.base_model import BaseModel
from models.base import Base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, DateTime, Numeric, Boolean
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship


class TransaccionesEntity(Base, BaseModel):
    __tablename__ = 'transacciones'
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Relación hacia la factura (uno)
    id_factura = Column(Integer, ForeignKey('facturas.id'), nullable=False)
    factura = relationship("FacturasEntity", back_populates="transaccion")

    id_cliente = Column(Integer, ForeignKey('clientes.id'), nullable=True)
    cliente = relationship("ClientesEntity", back_populates="transaccion")

    id_articulo = Column(Integer, ForeignKey("articulos.id"), nullable=False)
    articulo = relationship("ArticulosEntity", back_populates="transaccion")

    id_vendedor = Column(Integer, ForeignKey('vendedores.id'), nullable=True)
    vendedor = relationship("VendedoresEntity", back_populates="transaccion")


    subtotal_articulo = Column(Numeric(14, 4))
    cantidad_articulo = Column(Integer)
