from typing import Callable, Optional

import pandas as pd
from sqlalchemy.orm import relationship, Session, Query

from datawarehouse.models.base_model import BaseModel
from datawarehouse.models.base import Base

from sqlalchemy import Column, String, DateTime, func, BigInteger, Boolean, UniqueConstraint

from datawarehouse.models.zoho.dim_usuarios import DimUsuariosEntity


class DimLocalidadesEntity(Base, BaseModel):
    __tablename__  = 'dim_localidades'
    __table_args__ = (
        UniqueConstraint(
            'id_provincia', 'id_canton', 'id_parroquia',
            name='ux_localidad_logical_key',
        ),
        {'schema': 'operatividad'}
    )
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    id_provincia = Column(BigInteger, nullable=False)
    id_canton = Column(BigInteger, nullable=False)
    id_parroquia = Column(BigInteger, nullable=False)

    name_provincia = Column(String(256), nullable=False)
    name_canton = Column(String(256), nullable=False)
    name_parroquia = Column(String(256), nullable=False)

    usuarios_localidad = relationship(
        "DimUsuariosEntity",
        foreign_keys=[DimUsuariosEntity.id_localidad],
        back_populates='localidad_relationship',
        cascade="all, delete-orphan"
    )

    @classmethod
    def fetch_id_map(
            cls,
            session: Session,
            where_func: Optional[Callable[[Query], Query]] = None
    ):
        query = session.query(
            DimLocalidadesEntity.id,
            DimLocalidadesEntity.id_provincia,
            DimLocalidadesEntity.id_canton,
            DimLocalidadesEntity.id_parroquia,
        )

        if where_func:
            query = where_func(query)

        # 3. Ejecutar la query, obteniendo solo los datos filtrados
        rows = query.all()
        df_localidades = pd.DataFrame(rows, columns=['id_localidad', 'id_provincia', 'id_canton', 'id_parroquia'])
        return df_localidades



