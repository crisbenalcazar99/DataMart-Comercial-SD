import os
from pathlib import Path


def load_sql_statement(query_name):
    query_path = get_proyect_root() / "include" / "sql" / "comercial" / query_name
    with open(query_path, 'r') as file:
        return file.read()


def load_sql_statement_comercial(folder_name, query_name):
    query_path = get_proyect_root() / "include" / "sql" / "comercial" / folder_name / query_name
    with open(query_path, 'r') as file:
        return file.read()


def load_sql_statement_operatividad(folder_name, query_name):
    query_path = get_proyect_root() / "include" / "sql" / "operatividad" / folder_name / query_name
    with open(query_path, 'r') as file:
        return file.read()


def list_in_string(list_objects):
    return tuple(f"'{value}'" for value in list_objects)


def get_proyect_root() -> Path:
    return Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parent.parent))
