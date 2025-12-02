from datawarehouse.models.base_model import BaseModel
from datawarehouse.models.base import Base
from sqlalchemy import Column, Integer, Numeric, Boolean, DateTime, func, String
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship


class FactDetalleTransaccionesEntity(Base, BaseModel):
    __tablename__ = 'transacciones'
    __table_args__ = {'schema': 'comercial_info'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    estado_registro = Column(Boolean, default=True)
    id_transaction = Column(String(64), unique=True, nullable=False)

    # Relación hacia la factura (uno)
    id_factura = Column(Integer, ForeignKey('comercial_info.facturas.id'), nullable=False)
    factura = relationship("FactFacturasEntity", back_populates="transaccion")

    id_cliente = Column(Integer, ForeignKey('comercial_info.clientes.id'), nullable=True)
    cliente = relationship("DimClientesEntity", back_populates="transaccion")

    id_articulo = Column(Integer, ForeignKey("comercial_info.articulos.id"), nullable=False)
    articulo = relationship("DimArticulosEntity", back_populates="transaccion")

    id_vendedor = Column(Integer, ForeignKey('comercial_info.vendedores.id'), nullable=True)
    vendedor = relationship("DimVendedoresEntity", back_populates="transaccion")

    tipo_vendedor = Column(Integer, ForeignKey('comercial_info.catalogos.codigo_catalogo'), nullable=False)
    tipo_vendedor_relationship = relationship(
        'CatalogosEntity',
        foreign_keys=[tipo_vendedor],
        back_populates='transaccion_tipo_vendedor'
    )


    subtotal_articulo = Column(Numeric(14, 4))
    cantidad_articulo = Column(Integer)

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
