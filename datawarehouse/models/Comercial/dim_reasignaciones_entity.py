from datawarehouse.models.base_model import BaseModel
from datawarehouse.models.base import Base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey, Boolean
from sqlalchemy.orm import relationship


class DimReasignacionesEntity(Base, BaseModel):
    __tablename__ = 'reasignaciones'
    __table_args__ = {'schema': 'comercial_info'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    estado_registro = Column(Boolean, default=True)
    date_init = Column(DateTime, nullable=False)
    date_end = Column(DateTime, nullable=False)
    motivo = Column(String(255))

    fuente = Column(Integer, ForeignKey('comercial_info.catalogos.codigo_catalogo'), nullable=False)
    fuente_relationship = relationship(
        "CatalogosEntity",
        foreign_keys=[fuente],
        back_populates="reasignacion_fuente"
    )

    tipo_asignacion = Column(Integer, ForeignKey('comercial_info.catalogos.codigo_catalogo'), nullable=False)
    tipo_relationship = relationship(
        "CatalogosEntity",
        foreign_keys=[tipo_asignacion],
        back_populates="reasignacion_tipo"
    )

    id_vendedor = Column(Integer, ForeignKey("comercial_info.vendedores.id"), nullable=True)
    vendedor = relationship("DimVendedoresEntity", back_populates="reasignacion")

    id_cliente = Column(Integer, ForeignKey("comercial_info.clientes.id"), nullable=True)
    cliente = relationship("DimClientesEntity", back_populates="reasignacion")

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
    def get_vendedor_id(self, session: Session, where_func=None):
        query = session.query(DimReasignacionesEntity.id_vendedor)

        if where_func:
            query = where_func(query)

        return query.scalar()
