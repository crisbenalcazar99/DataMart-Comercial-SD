from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text


# ---------------------------------------------------------
# Configuración de conexión (AJUSTA TUS VALORES)
# ---------------------------------------------------------
PG_USER = "postgres"
PG_PASS = "#.P0stgres*.ec.*3ecurity#."
PG_HOST = "192.168.1.247"
PG_PORT = "5000"
PG_DB   = "security_data"

# psycopg3 usa el driver 'psycopg'
CONNECTION_URL = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)


def prueba_conexion_postgres():
    print("Intentando conectar a PostgreSQL...")

    try:
        engine = create_engine(CONNECTION_URL, echo=False)

        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()

        print("✅ Conexión exitosa a PostgreSQL.")
        print(f"Versión del servidor PostgreSQL:\n{version[0]}")

    except Exception as e:
        print("❌ Error al conectar a PostgreSQL:")
        print(e)
        raise


with DAG(
    dag_id="dag_test_camunda_airflow",
    description="DAG para validar conexión a una base PostgreSQL CAMUNDA",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["prueba", "postgres", "CAMUNDA"],
) as dag:

    test_pg = PythonOperator(
        task_id="test_conexion_postgres",
        python_callable=prueba_conexion_postgres
    )
