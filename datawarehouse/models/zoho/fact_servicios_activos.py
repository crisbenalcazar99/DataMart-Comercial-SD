from datawarehouse.models.base_model import BaseModel
from datawarehouse.models.base import Base
from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey, BigInteger, UniqueConstraint
from sqlalchemy.orm import relationship, Session


class FactServiciosActivosEntity(Base, BaseModel):
    __tablename__ = 'current_products'
    __table_args__ = (
        UniqueConstraint(
            'id_user', 'producto', 'tipo_firma', 'id_ruc_aux',
            name='ux_current_products_logical_key',
            # postgresql_nulls_not_distinct=True RECORDAR QUE DEBE SER NULL DISTINCR< CREAR ESTA VALIDACION POR CODIGO DB
        ),
        {'schema': 'operatividad'}
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    producto = Column(String(128))
    medio = Column(String(128))
    vigencia = Column(String(10))
    tipo_firma = Column(String(8))
    serial_firma = Column(String(128))
    fecha_caducidad_max = Column(DateTime)
    fecha_emision = Column(DateTime)
    operador_creacion = Column(String(128))
    id_ruc_aux = Column(Integer, nullable=True)
    link_renovacion = Column(String(1024))
    id_tramite = Column(Integer)
    producto_especifico = Column(String(128))
    tipo_atencion = Column(String(128))
    medio_contacto = Column(String(128))
    grupo_operador = Column(String(128))
    creation_date = Column(
        DateTime(timezone=False),
        server_default=func.now(),   # Fecha/hora de creación (la pone la BD)
        nullable=False
    )

    update_date = Column(
        DateTime(timezone=False),
        server_default=func.now(),   # Valor inicial al crear
        onupdate=func.now(),         # Se actualiza en cada UPDATE
        nullable=False
    )

    id_user = Column(BigInteger, ForeignKey('operatividad.users.id'), nullable=False)
    users = relationship("DimUsuariosEntity", back_populates="current_products")

    id_ruc = Column(BigInteger, ForeignKey('operatividad.dim_rucs.id'), nullable=True)
    rucs = relationship("DimRucsEntity", back_populates="current_products")

    @classmethod
    def get_register_idtramite(cls, session: Session, where_func=None):
        query = session.query(
            FactServiciosActivosEntity.id_tramite
        )

        if where_func:
            query = where_func(query)

        # Devuelve una tuple plana
        return tuple(row[0] for row in query.all())
