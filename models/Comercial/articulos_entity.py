import pandas as pd
from sqlalchemy.dialects import postgresql

from models.base_model import BaseModel
from models.base import Base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, DateTime, Numeric, func, ForeignKey
from sqlalchemy.orm import relationship


class ArticulosEntity(Base, BaseModel):
    __tablename__ = 'articulos'
    id = Column(Integer, primary_key=True, autoincrement=True)
    cod_articulo = Column(String(30), nullable=False, unique=True)
    nom_articulo = Column(String(255), nullable=False)
    producto = Column(Integer, ForeignKey('catalogos.codigo_catalogo'), nullable=False)
    producto_relationship = relationship(
        "CatalogosEntity",
        foreign_keys=[producto],
        back_populates="articulo_producto"
    )
    tipo = Column(Integer, ForeignKey('catalogos.codigo_catalogo'), nullable=False)
    tipo_relationship = relationship(
        "CatalogosEntity",
        foreign_keys=[tipo],
        back_populates="articulo_tipo"
    )
    empresa_origen = Column(Integer, ForeignKey('catalogos.codigo_catalogo'), nullable=False)
    empresa_origen_relationship = relationship(
        'CatalogosEntity',
        foreign_keys=[empresa_origen],
        back_populates='articulo_empresa_origen'
    )


    transaccion = relationship("TransaccionesEntity", back_populates="articulo", cascade="all, delete-orphan")

    @classmethod
    def get_all_codigos_articulos(cls, session: Session):
        codigos_articulos = session.query(ArticulosEntity.cod_articulo).all()
        return tuple(codigo[0] for codigo in codigos_articulos)

    @classmethod
    def get_list_codigos_articulos(cls, session: Session, where_func=None):
        query = session.query(
            ArticulosEntity.cod_articulo
        )
        if where_func:
            query = where_func(query)


        # 3. Ejecutar la query, obteniendo solo los datos filtrados
        codigos_articulos = query.all()

        return tuple(codigo[0] for codigo in codigos_articulos)



    @classmethod
    def get_articulos(cls, session: Session, where_func=None):
        query = session.query(
            ArticulosEntity.id,
            ArticulosEntity.cod_articulo
        )

        if where_func:
            query = where_func(query)

        # 3. Ejecutar la query, obteniendo solo los datos filtrados
        rows =query.all()
        df_articulos = pd.DataFrame(rows, columns=['id_articulo', 'cod_articulo'])
        return df_articulos