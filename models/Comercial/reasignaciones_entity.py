from models.base_model import BaseModel
from models.base import Base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, DateTime, Numeric, func, ForeignKey
from sqlalchemy.orm import relationship


class ReasignacionesEntity(Base, BaseModel):
    __tablename__ = 'reasignaciones'

    id = Column(Integer, primary_key=True, autoincrement=True)
    creation_date = Column(DateTime(timezone=True),
                           server_default=func.now(),  # lo pone la BD al INSERT
                           nullable=False)
    date_init = Column(DateTime, nullable=False)
    date_end = Column(DateTime, nullable=False)
    motivo = Column(String(255))
    fuente = Column(Integer, ForeignKey('catalogos.id'), nullable=False)
    fuente_relationship = relationship(
        "CatalogosEntity",
        foreign_keys=[fuente],
        back_populates="reasignacion_fuente"
    )

    tipo_asignacion = Column(Integer, ForeignKey('catalogos.id'), nullable=False)
    tipo_relationship = relationship(
        "CatalogosEntity",
        foreign_keys=[tipo_asignacion],
        back_populates="reasignacion_tipo"
    )

    id_vendedor = Column(Integer, ForeignKey("vendedores.id"), nullable=True)
    vendedor = relationship("VendedoresEntity", back_populates="reasignacion")

    id_cliente = Column(Integer, ForeignKey("clientes.id"), nullable=True)
    cliente = relationship("ClientesEntity", back_populates="reasignacion")


    @classmethod
    def get_vendedor_id(self, session: Session, where_func=None):
        query = session.query(ReasignacionesEntity.id_vendedor)

        if where_func:
            query = where_func(query)

        return query.scalar()
