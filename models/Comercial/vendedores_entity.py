import pandas as pd

from models.base_model import BaseModel
from models.base import Base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, DateTime, Numeric, func
from sqlalchemy.orm import relationship


class VendedoresEntity(Base, BaseModel):
    __tablename__ = 'vendedores'
    id = Column(Integer, primary_key=True, autoincrement=True)
    cod_vendedor = Column(Integer, nullable=False, unique=True)
    nom_vendedor = Column(String(255), nullable=False)

    transaccion = relationship("TransaccionesEntity", back_populates="vendedor", cascade="all, delete-orphan")
    reasignacion = relationship("ReasignacionesEntity", back_populates="vendedor", cascade="all, delete-orphan")
    @classmethod
    def get_vendedores(cls, session: Session, where_func=None):
        query = session.query(
            VendedoresEntity.id,
            VendedoresEntity.cod_vendedor
        )

        if where_func:
            query = where_func(query)

        # 3. Ejecutar la query, obteniendo solo los datos filtrados
        rows = query.all()
        df_vendores = pd.DataFrame(rows, columns=['id_vendedor', 'cod_vendedor'])
        return df_vendores
