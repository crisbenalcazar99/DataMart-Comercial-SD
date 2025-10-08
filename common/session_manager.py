from common.db import SessionLocal, SessionPortal, SessionFenix
from contextlib import contextmanager


# session_manager.py
@contextmanager
def get_session(db_alias="default"):
    if db_alias == "PORTAL":
        session = SessionPortal()
    elif db_alias == 'FENIX':
        session = SessionFenix()
    else:
        session = SessionLocal()

    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
