from datawarehouse.models.base_model import BaseModel
from datawarehouse.models.base import Base

from sqlalchemy import Column, String, DateTime, func, BigInteger, Boolean

class DimLocalidades(Base, BaseModel):
    __tablename__  = 'dim_localidades'
    __table_args__ = {'schema': 'operatividad'}
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    id_provincia = Column(BigInteger, nullable=False)
    id_canton = Column(BigInteger, nullable=False)
    id_parroquia = Column(BigInteger, nullable=False)

    name_provincia = Column(String(256), nullable=False)
    name_canton = Column(String(256), nullable=False)
    name_parroquia = Column(String(256), nullable=False)

