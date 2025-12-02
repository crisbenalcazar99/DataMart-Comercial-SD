from datawarehouse.models.Comercial.dim_articulos_entity import DimArticulosEntity
from datawarehouse.models.Comercial.dim_reasignaciones_entity import DimReasignacionesEntity
from datawarehouse.models.Comercial.fact_detalle_transacciones_entity import FactDetalleTransaccionesEntity
from datawarehouse.models.base_model import BaseModel
from datawarehouse.models.base import Base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, func
from sqlalchemy.orm import relationship


class CatalogosEntity(Base, BaseModel):
    __tablename__ = 'catalogos'
    __table_args__ = {'schema': 'comercial_info'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    codigo_catalogo = Column(Integer, nullable=False, unique=True)
    mnemonico = Column(String(255), nullable=False)
    nombre = Column(String(255), nullable=False)
    estado_registro = Column(Boolean, default=True)

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
    reasignacion_fuente = relationship(
        "DimReasignacionesEntity",
        back_populates="fuente_relationship",
        foreign_keys=[DimReasignacionesEntity.fuente],
        cascade="all, delete-orphan"
    )

    reasignacion_tipo = relationship(
        "DimReasignacionesEntity",
        back_populates="tipo_relationship",
        foreign_keys=[DimReasignacionesEntity.tipo_asignacion],
        cascade="all, delete-orphan"
    )

    articulo_producto = relationship(
        'DimArticulosEntity',
        back_populates="producto_relationship",
        foreign_keys=[DimArticulosEntity.producto],
        cascade="all, delete-orphan"
    )

    articulo_tipo = relationship(
        "DimArticulosEntity",
        back_populates="tipo_relationship",
        foreign_keys=[DimArticulosEntity.tipo],
        cascade="all, delete-orphan"
    )

    articulo_empresa_origen = relationship(
        "DimArticulosEntity",
        back_populates="empresa_origen_relationship",
        foreign_keys=[DimArticulosEntity.empresa_origen],
        cascade="all, delete-orphan"
    )

    transaccion_tipo_vendedor = relationship(
        "FactDetalleTransaccionesEntity",
        back_populates="tipo_vendedor_relationship",
        foreign_keys=[FactDetalleTransaccionesEntity.tipo_vendedor],
        cascade="all, delete-orphan"
    )


