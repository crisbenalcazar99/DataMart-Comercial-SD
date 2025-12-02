import pandas as pd

from datawarehouse.models.base_model import BaseModel
from datawarehouse.models.base import Base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, DateTime, func, Boolean
from sqlalchemy.orm import relationship


class DimVendedoresEntity(Base, BaseModel):
    __tablename__ = 'vendedores'
    __table_args__ = {'schema': 'comercial_info'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    estado_registro = Column(Boolean, default=True)
    cod_vendedor = Column(Integer, nullable=False, unique=True)
    nom_vendedor = Column(String(255), nullable=False)
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

    transaccion = relationship("FactDetalleTransaccionesEntity", back_populates="vendedor", cascade="all, delete-orphan")
    reasignacion = relationship("DimReasignacionesEntity", back_populates="vendedor", cascade="all, delete-orphan")
    @classmethod
    def get_vendedores(cls, session: Session, where_func=None):
        query = session.query(
            DimVendedoresEntity.id,
            DimVendedoresEntity.cod_vendedor
        )

        if where_func:
            query = where_func(query)

        # 3. Ejecutar la query, obteniendo solo los datos filtrados
        rows = query.all()
        df_vendores = pd.DataFrame(rows, columns=['id_vendedor', 'cod_vendedor'])
        return df_vendores
