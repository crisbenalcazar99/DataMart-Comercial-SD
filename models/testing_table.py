from models.base_model import BaseModel
from models.base import Base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, DateTime, Numeric, func


class TestingBase(Base, BaseModel):
    __tablename__ = 'testing_table'
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Columnas tipo string (texto libre)
    cedula = Column(String(20), nullable=True)
    factura = Column(String(30), nullable=True)
    ruc = Column(String(20), nullable=True)
    razon_social = Column(String(255), nullable=True)
    operador_creacion = Column(String(100), nullable=True)
    serial_firma = Column(String(100), nullable=True)
    correo = Column(String(150), nullable=True)
    telefono = Column(String(30), nullable=True)
    nombre = Column(String(150), nullable=True)
    operador_aprobacion = Column(String(100), nullable=True)

    # Columnas categóricas (pueden ser texto con valores limitados)
    portal_origen = Column(String(50), nullable=True)
    vigencia = Column(String(20), nullable=True)
    producto = Column(String(50), nullable=True)
    medio_firma = Column(String(50), nullable=True)
    tipo_persona = Column(String(50), nullable=True)
    medio_contacto = Column(String(50), nullable=True)
    flujo = Column(String(50), nullable=True)
    atencion = Column(String(50), nullable=True)

    # Columnas de tipo fecha/hora
    fecha_aprobacion = Column(DateTime, nullable=True)
    fecha_expedicion = Column(DateTime, nullable=True)
    fecha_facturacion = Column(DateTime, nullable=True)
    fecha_inicio_tramite = Column(DateTime, nullable=True)
    fecha_caducidad = Column(DateTime, nullable=True)

    valor_factura = Column(Numeric(12, 2))

    @classmethod
    def search_max_aprobation_date(cls, session: Session):
        return session.query(func.max(cls.fecha_aprobacion)).scalar()

