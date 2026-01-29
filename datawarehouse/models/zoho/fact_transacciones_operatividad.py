from datawarehouse.models.base_model import BaseModel
from datawarehouse.models.base import Base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey, BigInteger
from sqlalchemy.orm import relationship


class FactTransaccionesOperatividadEntity(Base, BaseModel):
    __tablename__ = 'transaction_history'
    __table_args__ = {'schema': 'operatividad'}
    # __table_args__ = (
    #     #UniqueConstraint('id_user', 'ruc', 'tipo_firma', name='ux_id_user_ruc_tipo_firma'),
    #     {'schema': 'operatividad'}
    # )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    producto = Column(String(128))
    medio = Column(String(128))
    vigencia = Column(String(10))
    tipo_firma = Column(String(8))
    serial_firma = Column(String(128), unique=True)
    estado_firma = Column(String(128))
    fecha_aprobacion = Column(DateTime)
    fecha_caducidad = Column(DateTime)
    fecha_emision = Column(DateTime)
    operador_creacion = Column(String(128))
    link_renovacion = Column(String(1024))
    producto_especifico = Column(String(128))
    id_tramite = Column(Integer)
    tipo_atencion = Column(String(128))
    medio_contacto = Column(String(128))
    grupo_operador = Column(String(128))
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
    #id_firma = Column(Integer)

    id_user = Column(BigInteger, ForeignKey('operatividad.users.id'), nullable=False)
    users = relationship("DimUsuariosEntity", back_populates="transaction_history")

    id_ruc = Column(BigInteger, ForeignKey('operatividad.dim_rucs.id'), nullable=True)
    rucs = relationship("DimRucsEntity", back_populates="transaction_history")

    @classmethod
    def get_last_transaction_date(cls, session: Session, where_func=None):
        query = session.query(
            func.max(FactTransaccionesOperatividadEntity.fecha_aprobacion)
        )
        if where_func:
            query = where_func(query)

        result = query.scalar()
        return str(result) if result else None

    @classmethod
    def get_register_idtramite(cls, session: Session, where_func=None):
        query = session.query(
            FactTransaccionesOperatividadEntity.id_tramite
        )

        if where_func:
            query = where_func(query)

        # Devuelve una tuple plana
        return tuple(row[0] for row in query.all())

