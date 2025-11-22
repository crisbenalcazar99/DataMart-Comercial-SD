from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def tarea_prueba():
    print("🎉 Airflow está funcionando correctamente!")


with DAG(
    dag_id="dag_prueba_basica",
    description="DAG básico para validar que Airflow funciona",
    schedule=timedelta(days=1),  # ← reemplaza schedule_interval por schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["prueba", "validacion"],
) as dag:

    tarea_1 = PythonOperator(
        task_id="imprimir_mensaje",
        python_callable=tarea_prueba
    )

    tarea_2 = PythonOperator(
        task_id="tarea_final",
        python_callable=lambda: print("✔️ Segunda tarea ejecutada")
    )

    tarea_1 >> tarea_2
