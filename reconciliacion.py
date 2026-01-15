from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='proceso_reconciliacion_datos',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Tarea 1: Ejecutar el script de R que descarga y compara
    run_comparison = BashOperator(
        task_id='comparar_fuentes',
        bash_command='Rscript /ruta/a/tu/script_comparacion.R'
    )

    # Tarea 2: (Opcional) Enviar notificación si hay discrepancias
    # run_comparison >> send_alert_task
