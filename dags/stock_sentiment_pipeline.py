from datetime import datetime, timedelta
import os
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from databricks.sdk import WorkspaceClient

# Configuración por defecto del DAG
default_args = {
    'owner': 'fabian',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SYMBOLS = ("AAPL", "MSFT", "GOOGL", "AMZN")

def fetch_market_news(**context):
    """Tarea 1: Obtiene las noticias de Alpha Vantage."""
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    tickers_string = ",".join(SYMBOLS)
    url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={tickers_string}&apikey={api_key}"
    
    print(f"Buscando noticias para: {tickers_string}")
    response = requests.get(url)
    
    if response.status_code != 200:
        raise Exception(f"Error HTTP: {response.status_code}")
        
    data = response.json()
    if "Note" in data or "Information" in data:
        raise Exception(f"Límite de API excedido: {data.get('Note') or data.get('Information')}")
        
    # Guardamos el JSON en el XCom de Airflow para pasarlo a la siguiente tarea
    return data

def save_and_upload_news(**context):
    """Tarea 2 & 3: Guarda localmente y sube al Unity Catalog Volume de Databricks."""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='fetch_news_task')
    
    if not data:
        raise ValueError("No hay datos recibidos de la tarea anterior.")
        
    now = datetime.now()
    year, month, day, hour = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d"), now.strftime("%H")

    # Ruta local dentro del contenedor montado
    raw_path = f"/opt/airflow/data/raw/news_bulk/year={year}/month={month}/day={day}/hour={hour}"
    os.makedirs(raw_path, exist_ok=True)

    bulk_filename = f"bulk_news_{now.strftime('%Y%m%d_%H%M%S')}.json"
    file_path = os.path.join(raw_path, bulk_filename)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"✅ Archivo guardado localmente en: {file_path}")

    # Subida a Databricks
    w = WorkspaceClient()
    remote_path = f"/Volumes/workspace/default/stockify_vol/raw/news_bulk/year={year}/month={month}/day={day}/hour={hour}/{bulk_filename}"
    
    print(f"🚀 Subiendo a Databricks: {remote_path}")
    with open(file_path, "rb") as f:
        w.files.upload(remote_path, f, overwrite=True)
    print("✅ Sincronización completada con éxito.")

# Definición del DAG
with DAG(
    'stock_sentiment_pipeline',
    default_args=default_args,
    description='Pipeline automatizado de sentimiento bursátil con Alpha Vantage y Databricks',
    schedule_interval='0 */12 * * *',  # Corre cada 12 horas (2 veces al día como querías)
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_news_task',
        python_callable=fetch_market_news,
    )

    upload_task = PythonOperator(
        task_id='save_and_upload_task',
        python_callable=save_and_upload_news,
    )

    # Flujo de ejecución
    fetch_task >> upload_task