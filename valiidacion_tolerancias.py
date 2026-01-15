from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime

# Función que realiza la validación técnica
def validar_conciliacion_contable(tolerance_percent=1.0):
    # 1. Conexión a las fuentes usando Hooks
    sql_hook = MsSqlHook(mssql_conn_id='mssql_contable')
    
    # Obtener suma de la fuente Contable (Libro Mayor)
    contable_res = sql_hook.get_first("SELECT SUM(debe) FROM Facturas WHERE mes = '2023-12'")
    suma_contable = float(contable_res[0]) if contable_res[0] else 0

    # Obtener suma de la fuente Operativa (Ventas Reales)
    operativa_res = sql_hook.get_first("SELECT SUM(monto) FROM Ventas_Detalle WHERE mes = '2023-12'")
    suma_operativa = float(operativa_res[0]) if operativa_res[0] else 0

    # 2. Cálculo de la diferencia
    if suma_contable == 0:
        raise ValueError("Error: La suma contable es 0. Revisar origen.")

    diferencia = abs(suma_contable - suma_operativa)
    porcentaje_error = (diferencia / suma_contable) * 100

    print(f"Suma Contable: {suma_contable} | Suma Operativa: {suma_operativa}")
    print(f"Diferencia: {diferencia} ({porcentaje_error:.2f}%)")

    # 3. Lógica del Quality Gate
    if porcentaje_error > tolerance_percent:
        raise Exception(f"CRÍTICO: La diferencia ({porcentaje_error:.2f}%) supera la tolerancia del {tolerance_percent}%.")
    
    print("Validación exitosa. Procediendo al cierre.")

# Definición del DAG
with DAG(
    'cierre_contable_con_validacion',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@monthly',
    catchup=False
) as dag:

    tarea_validacion = PythonOperator(
        task_id='validar_tolerancia_contable',
        python_callable=validar_conciliacion_contable,
        op_kwargs={'tolerance_percent': 1.0}
    )

    # Solo si la validación pasa, se ejecutan las siguientes tareas
    generar_reporte_shiny = PythonOperator(
        task_id='preparar_reporte_final',
        python_callable=lambda: print("Generando archivo para Shiny...")
    )

    tarea_validacion >> generar_reporte_shiny









from airflow.operators.email import EmailOperator

def notify_failure(context):
    # Extraemos información del error para el correo
    task_instance = context['task_instance']
    error_msg = f"Fallo en la tarea: {task_instance.task_id}\nDAG: {task_instance.dag_id}"
    
    send_email = EmailOperator(
        task_id='send_email_alert',
        to='jefe_contabilidad@empresa.com',
        subject='⚠️ ALERTA: Diferencia en Conciliación Excedida',
        html_content=f"<h3>Error detectado</h3><p>{error_msg}</p>"
    )
    return send_email.execute(context=context)

# Aplicar al DAG
with DAG(
    'cierre_contable_con_alertas',
    default_args={'on_failure_callback': notify_failure}, # <-- Alerta global
    ...
) as dag:
    # Tus tareas de validación aquí...
    
    
    
    
    
    
    
    
    def registrar_auditoria(suma_contable, suma_operativa, diferencia, status):
    sql_hook = MsSqlHook(mssql_conn_id='mssql_auditoria')
    
    insert_query = """
    INSERT INTO LOG_CONCILIACIONES (fecha_ejecucion, fuente_a, fuente_b, diferencia, estado)
    VALUES (GETDATE(), %s, %s, %s, %s)
    """
    sql_hook.run(insert_query, parameters=(suma_contable, suma_operativa, diferencia, status))
