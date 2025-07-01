import time
import json
import requests
import sys
import os
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from loguru import logger

# Carica variabili d'ambiente
load_dotenv()

# Configurazione
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
KAFKA_TOPIC = 'water_metrics_raw'
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL', 300))  # 5 minuti di default

# Configurazione API USGS
# Stazione 09380000: Colorado River at Lees Ferry, AZ
SITE_ID = os.getenv('USGS_SITE_ID', '09380000')
PARAMETER_CODES = '00400,63680'  # pH e torbidità
USGS_API_URL = f"https://waterservices.usgs.gov/nwis/iv/?format=json&sites={SITE_ID}&parameterCd={PARAMETER_CODES}"

# Configurazione logger
logger.remove()
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")
logger.add("water_metrics_producer.log", rotation="10 MB", level="DEBUG")

def create_kafka_producer():
    """Crea e restituisce un producer Kafka."""
    retries = 5
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.success("Connessione a Kafka stabilita con successo!")
            return producer
        except NoBrokersAvailable:
            logger.error(f"Impossibile connettersi a Kafka. Tentativo {i+1} di {retries}...")
            time.sleep(5)
    logger.critical("Connessione a Kafka fallita dopo diversi tentativi.")
    sys.exit(1)

def fetch_usgs_data():
    """Scarica e processa i dati di qualità dell'acqua dall'API USGS."""
    logger.info(f"Chiamata all'API USGS per la stazione {SITE_ID}...")
    try:
        headers = {'User-Agent': 'MarinePollutionProject/1.0'}
        
        # Se hai un token API USGS, usalo qui
        usgs_api_token = os.getenv('USGS_API_TOKEN')
        if usgs_api_token:
            headers['Authorization'] = f'Bearer {usgs_api_token}'
            
        response = requests.get(USGS_API_URL, timeout=20, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        time_series = data.get('value', {}).get('timeSeries', [])
        if not time_series:
            logger.warning(f"L'API USGS non ha restituito dati per la stazione {SITE_ID}.")
            return None

        latest_metrics = {'sensor_id': SITE_ID}
        
        for ts in time_series:
            param_code = ts['variable']['variableCode'][0]['value']
            
            if ts['values'] and ts['values'][0]['value']:
                last_value = ts['values'][0]['value'][0]
                value = float(last_value['value'])
                timestamp = last_value['dateTime']
                
                if param_code == '00400':  # pH
                    latest_metrics['pH'] = value
                elif param_code == '63680':  # Torbidità
                    latest_metrics['turbidity'] = value
                
                latest_metrics['timestamp'] = timestamp

        if 'pH' not in latest_metrics and 'turbidity' not in latest_metrics:
            logger.warning("Nessuna metrica di pH o torbidità trovata nei dati ricevuti.")
            return None
            
        return latest_metrics

    except requests.RequestException as e:
        logger.error(f"Errore durante la richiesta HTTP: {e}")
        return None
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        logger.error(f"Errore durante il parsing della risposta JSON: {e}")
        return None

def simulate_water_metrics():
    """Genera dati simulati di qualità dell'acqua."""
    metrics = {
        'sensor_id': SITE_ID,
        'timestamp': datetime.now().isoformat(),
        'lat': 36.865,  # Coordinate della stazione USGS
        'lon': -111.588,
        'pH': round(random.uniform(6.5, 8.5), 2),  # Range normale di pH per acque naturali
        'turbidity': round(random.uniform(0.5, 5.0), 2),  # NTU (Unità di Torbidità Nefelometrica)
        'temperature_c': round(random.uniform(10.0, 25.0), 1),  # Temperatura in Celsius
        'dissolved_oxygen': round(random.uniform(7.0, 14.0), 2),  # mg/L
        'conductivity': round(random.uniform(200, 800), 0),  # µS/cm
        'nitrate': round(random.uniform(0.1, 3.0), 2),  # mg/L
    }
    
    # Occasionalmente genera valori anomali per simulare inquinamento
    if random.random() < 0.1:  # 10% di probabilità
        anomaly_type = random.choice(['pH', 'turbidity', 'nitrate'])
        if anomaly_type == 'pH':
            metrics['pH'] = random.choice([round(random.uniform(4.0, 6.0), 2), round(random.uniform(9.0, 10.0), 2)])
        elif anomaly_type == 'turbidity':
            metrics['turbidity'] = round(random.uniform(10.0, 30.0), 2)
        elif anomaly_type == 'nitrate':
            metrics['nitrate'] = round(random.uniform(5.0, 15.0), 2)
    
    return metrics

def main():
    """Ciclo principale del producer."""
    producer = create_kafka_producer()
    
    logger.info(f"Producer avviato. Invio dati dalla stazione USGS {SITE_ID} al topic '{KAFKA_TOPIC}'.")
    
    while True:
        try:
            # Tenta di ottenere dati reali
            water_data = fetch_usgs_data()
            
            if water_data:
                logger.info(f"Dati reali ricevuti: {json.dumps(water_data)}")
                producer.send(KAFKA_TOPIC, value=water_data)
                producer.flush()
                logger.success(f"Dati reali inviati con successo a Kafka.")
            else:
                # Se i dati reali non sono disponibili, usa dati simulati
                simulated_data = simulate_water_metrics()
                logger.info(f"Dati simulati generati: {json.dumps(simulated_data)}")
                producer.send(KAFKA_TOPIC, value=simulated_data)
                producer.flush()
                logger.success(f"Dati simulati inviati con successo a Kafka.")

            logger.info(f"Prossimo controllo tra {POLL_INTERVAL_SECONDS / 60} minuti...")
            time.sleep(POLL_INTERVAL_SECONDS)
            
        except KeyboardInterrupt:
            logger.info("Rilevato arresto manuale. Arrivederci!")
            break
        except Exception as e:
            logger.error(f"Errore inaspettato nel ciclo principale: {e}")
            time.sleep(10)

if __name__ == '__main__':
    main()
