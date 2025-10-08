from models.Comercial.articulos_entity import ArticulosEntity
from models.Comercial.reasignaciones_entity import ReasignacionesEntity
from models.base_model import BaseModel
from models.base import Base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, DateTime, Numeric, func, Boolean
from sqlalchemy.orm import relationship


class CatalogosEntity(Base, BaseModel):
    __tablename__ = 'catalogos'
    id = Column(Integer, primary_key=True, autoincrement=True)
    codigo_catalogo = Column(Integer, nullable=False, unique=True)
    mnemonico = Column(String(255), nullable=False)
    nombre = Column(String(255), nullable=False)
    estado_registro = Column(Boolean, default=True)

    # Relación inversa (uno a muchos)
    reasignacion_fuente = relationship(
        "ReasignacionesEntity",
        back_populates="fuente_relationship",
        foreign_keys=[ReasignacionesEntity.fuente],
        cascade="all, delete-orphan"
    )

    reasignacion_tipo = relationship(
        "ReasignacionesEntity",
        back_populates="tipo_relationship",
        foreign_keys=[ReasignacionesEntity.tipo_asignacion],
        cascade="all, delete-orphan"
    )

    articulo_producto = relationship(
        'ArticulosEntity',
        back_populates="producto_relationship",
        foreign_keys=[ArticulosEntity.producto],
        cascade="all, delete-orphan"
    )

    articulo_tipo = relationship(
        "ArticulosEntity",
        back_populates="tipo_relationship",
        foreign_keys=[ArticulosEntity.tipo],
        cascade="all, delete-orphan"
    )

    articulo_empresa_origen = relationship(
        "ArticulosEntity",
        back_populates="empresa_origen_relationship",
        foreign_keys=[ArticulosEntity.empresa_origen],
        cascade="all, delete-orphan"
    )


