from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session
import pandas as pd
from sqlalchemy.dialects.postgresql import insert


class BaseModel:
    """
    Clase base para modelos ORM con métodos utilitarios comunes.
    """

    def persist(self, session: Session):
        session.add(self)

    @classmethod
    def persist_multiple(cls, session: Session, objetos: list):
        session.add_all(objetos)

    @classmethod
    def search_by_id(cls, session: Session, id_):
        return session.get(cls, id_)

    """
    @classmethod
    def persist_from_dataframe(cls, session: Session, df: pd.DataFrame):
        objetos = [cls(**row.where(pd.notnull(row), None)) for _, row in df.iterrows()]
        cls.persist_multiple(session, objetos)
    """

    @classmethod
    def persist_from_dataframe(cls, session: Session, df: pd.DataFrame,
                               conflict_cols=None, mode="INSERT", update_cols=None):
        """
        Inserta datos desde un DataFrame.
        - conflict_cols: lista de columnas para identificar conflictos (por ejemplo, ['cod_cliente'])
        - mode: 'INSERT' (default, solo inserta), 'IGNORE' (ignora si hay conflicto), 'UPDATE' (actualiza si hay conflicto)
        - update_cols: columnas a actualizar en modo update (si None, actualiza todas excepto las de conflicto)
        """

        records = df.where(pd.notnull(df), None).to_dict(orient='records')
        if not records:
            return

        if conflict_cols and mode in ["IGNORE", "UPDATE"]:
            stmt = insert(cls.__table__).values(records)
            if mode == "IGNORE":
                stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
            elif mode == "UPDATE":
                if update_cols is None:
                    update_cols = [col for col in df.columns if col not in conflict_cols]
                update_dict = {col: getattr(stmt.excluded, col) for col in update_cols}
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_cols,
                    set_=update_dict
                )

            session.execute(stmt)
            session.commit()
        else:
            # Modo básico: solo inserta (se puede duplicar si no hay restricciones en DB)
            objetos = [cls(**row) for row in records]
            cls.persist_multiple(session, objetos)
            session.commit()
