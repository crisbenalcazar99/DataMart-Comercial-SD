import pandas as pd

from datawarehouse.models.base_model import BaseModel
from datawarehouse.models.base import Base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import relationship


class ClientesEntity(Base, BaseModel):
    __tablename__ = 'clientes'
    #__table_args__ = {'schema': 'comercial_info'}
    id = Column(Integer, primary_key=True, autoincrement=True)
    cod_cliente = Column(String(30), nullable=False, unique=True)
    cliente = Column(String(255), nullable=False)
    cif = Column(String(15), nullable=False, unique=False)
    creation_date = Column(DateTime)
    update_date = Column(DateTime)
    transaccion = relationship("TransaccionesEntity", back_populates="cliente", cascade="all, delete-orphan")
    reasignacion = relationship("ReasignacionesEntity", back_populates="cliente", cascade="all, delete-orphan")
    @classmethod
    def get_clientes(cls, session: Session, where_func=None):

        query = session.query(
            ClientesEntity.id,
            ClientesEntity.cif
        )

        if where_func:
            query = where_func(query)

        # 3. Ejecutar la query, obteniendo solo los datos filtrados
        rows = query.all()
        df_clientes = pd.DataFrame(rows, columns=['id_cliente', 'cif'])
        return df_clientes

    @classmethod
    def get_clientes_update(cls, session: Session, where_func=None):

        query = session.query(
            ClientesEntity.cif,
            ClientesEntity.update_date
        )

        if where_func:
            query = where_func(query)

        rows = query.all()
        df_clientes = pd.DataFrame(rows, columns=['cif', 'update_date'])
        return df_clientes

    @classmethod
    def get_cliente_id(cls, session: Session, where_func=None):
        query = session.query(ClientesEntity.id)

        if where_func:
            query = where_func(query)

        return query.scalar()


