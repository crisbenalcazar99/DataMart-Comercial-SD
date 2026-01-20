from typing import Optional, Callable

import pandas as pd

from datawarehouse.models.base_model import BaseModel
from datawarehouse.models.base import Base
from sqlalchemy.orm import Session, Query
from sqlalchemy import Column, String, DateTime, func, BigInteger, ForeignKey, Integer
from sqlalchemy.orm import relationship


class DimUsuariosEntity(Base, BaseModel):
    __tablename__ = 'users'
    __table_args__ = {'schema': 'operatividad'}
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    cedula = Column(String(32), unique=True)
    apellido_paterno = Column(String(128))
    apellido_materno = Column(String(128))
    nombre = Column(String(128))
    email = Column(String(255))
    phone = Column(String(255))
    fecha_nacimiento = Column(DateTime)
    profesion = Column(String(255))
    security_points = Column(Integer)

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

    # Relacion una a muchos
    transaction_history = relationship('FactTransaccionesOperatividadEntity', back_populates="users", cascade="all, delete-orphan")
    current_products = relationship('FactServiciosActivosEntity', back_populates='users', cascade='all, delete-orphan')

    id_localidad = Column(BigInteger, ForeignKey('operatividad.dim_localidades.id'), nullable=True)
    localidad_relationship = relationship(
        'DimLocalidadesEntity',
        foreign_keys=[id_localidad],
        back_populates='usuarios_localidad'
    )



    @classmethod
    def get_users(cls, session: Session, where_func=None):
        query = session.query(
            DimUsuariosEntity.id,
            DimUsuariosEntity.cedula
        )

        if where_func:
            query = where_func(query)

        # 3. Ejecutar la query, obteniendo solo los datos filtrados
        rows = query.all()
        df_users = pd.DataFrame(rows, columns=['id_user', 'cedula'])
        return df_users

    @classmethod
    def fetch_id_map(
            cls,
            session: Session,
            where_func: Optional[Callable[[Query], Query]] = None
    ):
        query = session.query(
            DimUsuariosEntity.id,
            DimUsuariosEntity.cedula
        )

        if where_func:
            query = where_func(query)

        # 3. Ejecutar la query, obteniendo solo los datos filtrados
        rows = query.all()
        df_users = pd.DataFrame(rows, columns=['id_user', 'cedula'])
        return df_users
