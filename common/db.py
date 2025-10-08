from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common.connection import build_connection_url
from config.setting import get_db_config

# Configuracion para las Bases de Datos
DB_CONFIG_PORTAL = get_db_config("PORTAL")
DB_CONFIG_LOCAL = get_db_config("LOCAL")
DB_CONFIG_FENIX = get_db_config("FENIX")


# Engine y session para la base principal

engine = create_engine(build_connection_url(DB_CONFIG_LOCAL))
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Engine y session para la base de datos del portal
engine_portal = create_engine(build_connection_url(DB_CONFIG_PORTAL), echo=True, future=True)
SessionPortal = sessionmaker(autocommit=False, autoflush=False, bind=engine_portal)

# Engine y session para la base de datos del Fenix
engine_fenix = create_engine(build_connection_url(DB_CONFIG_FENIX))
SessionFenix = sessionmaker(autocommit=False, autoflush=False, bind=engine_fenix)
