import os
import json
import requests
from datetime import datetime

# API configuration
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY") 
SYMBOLS = ("AAPL")

def fetch_market_news():
    """Función 1: Realiza la petición HTTP a la API y devuelve el JSON."""
    tickers_string = ",".join(SYMBOLS)
    URL = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={tickers_string}&apikey={API_KEY}"
    
    print(f"[{datetime.now()}] Fetching market news from Alpha Vantage API for {len(SYMBOLS)} tickers...")
    response = requests.get(URL)

    if response.status_code != 200:
        print(f"[{datetime.now()}] Error fetching data: {response.status_code} - {response.text}")
        return None
    
    data = response.json()

    if "Note" in data or "Information" in data:
        print(f"[{datetime.now()}] API rate limit exceeded: {data.get('Note') or data.get('Information')}")
        return None
        
    return data

def save_raw_news(data):
    """Función 2: Guarda el JSON crudo en disco organizado por carpetas de fecha/hora."""
    if not data:
        print("No data to save.")
        return None
        
    now = datetime.now()
    year, month, day, hour = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d"), now.strftime("%H")

    raw_path = f"./data/raw/news_bulk/year={year}/month={month}/day={day}/hour={hour}"
    os.makedirs(raw_path, exist_ok=True)

    bulk_filename = f"bulk_news_{now.strftime('%Y%m%d_%H%M%S')}.json"
    file_path = os.path.join(raw_path, bulk_filename)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        
    print(f"✅ JSON data saved to {file_path}")
    return file_path

def process_news_in_memory(data):
    """Función 3: Procesa el JSON en memoria (extrae el feed de artículos)."""
    if not data:
        return
        
    articles = data.get("feed", [])
    print(f"📊 Found {len(articles)} articles. Processing in memory...")
    
    # Aquí puedes imprimir un par de títulos de prueba para verificar que llegaron
    for i, article in enumerate(articles[:3], 1):
        print(f"   {i}. {article.get('title')} (Source: {article.get('source')})")

# Bloque de ejecución principal
if __name__ == "__main__":
    print("--- INICIANDO PIPELINE DE INGESTA ---")
    raw_data = fetch_market_news()
    
    if raw_data:
        saved_path = save_raw_news(raw_data)
        process_news_in_memory(raw_data)
    print("--- PROCESO FINALIZADO ---")