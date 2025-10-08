from common.db import engine
from models.base import Base  # Tu declarative_base central
from models.base import Base


# common/schema_initializer.py
def create_schema():
    """
        Crea el esquema de la base de datos utilizando SQLAlchemy.
    """
    try:
        Base.metadata.create_all(engine)
        print("Esquema de base de datos creado exitosamente.")
    except Exception as e:
        print(f"Error al crear el esquema de base de datos: {e}")
        raise
