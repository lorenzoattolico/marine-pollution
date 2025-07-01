import time
import json
import requests
import random
import sys
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from loguru import logger

# Carica variabili d'ambiente
load_dotenv()

# Configurazione
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
KAFKA_TOPIC = 'buoy_raw'
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL', 300))  # 5 minuti di default
BUOY_ID = os.getenv('BUOY_ID', '13002')  # Boa NOAA nell'Atlantico di default

# Configurazione logger
logger.remove()
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")
logger.add("buoy_producer.log", rotation="10 MB", level="DEBUG")

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

def fetch_buoy_data():
    """Scarica e processa i dati della boa dalla NOAA."""
    logger.info(f"Chiamata alla boa NOAA: {BUOY_ID}")
    try:
        # Se hai un token API NOAA, usalo qui
        noaa_api_token = os.getenv('NOAA_API_TOKEN')
        headers = {'User-Agent': 'MarinePollutionProject/1.0'}
        if noaa_api_token:
            headers['token'] = noaa_api_token
            
        # URL NOAA per i dati delle boe
        url = f'https://www.ndbc.noaa.gov/data/realtime2/{BUOY_ID}.txt'
        response = requests.get(url, timeout=10, headers=headers)
        response.raise_for_status()

        lines = response.text.splitlines()
        if len(lines) < 3:
            logger.warning("Dati insufficienti dalla boa NOAA")
            return None

        # I dati NOAA hanno header nella prima riga e unità nella seconda
        header = lines[0].replace("#", "").split()
        latest_data = lines[2].split()

        data_dict = dict(zip(header, latest_data))

        processed_data = {}
        for key, value in data_dict.items():
            if value != 'MM':  # MM indica dato mancante
                try:
                    processed_data[key] = float(value)
                except ValueError:
                    processed_data[key] = value

        return processed_data

    except requests.RequestException as e:
        logger.error(f"Errore durante la richiesta HTTP: {e}")
        return None

def simulate_water_quality_metrics(data: dict):
    """Aggiunge metriche simulate di pH, torbidità e contaminanti ai dati reali."""
    # Simuliamo valori normali con occasionali anomalie
    if random.random() < 0.1:  # 10% di probabilità di anomalia
        data['pH'] = random.choice([5.5, 9.0])  # Valori anomali
    else:
        data['pH'] = round(random.uniform(7.8, 8.4), 2)  # Valori normali

    if random.random() < 0.1:
        data['turbidity'] = round(random.uniform(6.0, 10.0), 2)  # Anomalia alta torbidità
    else:
        data['turbidity'] = round(random.uniform(0.2, 1.5), 2)  # Normale

    if random.random() < 0.1:
        data['contaminant_ppm'] = round(random.uniform(8.0, 15.0), 2)  # Alta contaminazione
    else:
        data['contaminant_ppm'] = round(random.uniform(0.5, 2.0), 2)  # Normale

    return data

def main():
    """Ciclo principale del producer."""
    producer = create_kafka_producer()

    logger.info(f"Producer avviato. Invio dati dalla boa {BUOY_ID} al topic '{KAFKA_TOPIC}'.")

    while True:
        try:
            buoy_data = fetch_buoy_data()

            if buoy_data:
                buoy_data['sensor_id'] = BUOY_ID
                
                # Assicuriamoci che ci siano le coordinate
                if 'LAT' not in buoy_data and 'LON' not in buoy_data:
                    # Coordinate di default per la boa
                    buoy_data['LAT'] = 42.345
                    buoy_data['LON'] = -68.502

                final_data = simulate_water_quality_metrics(buoy_data)
                final_data['timestamp'] = int(time.time() * 1000)  # Timestamp in milliseconds

                logger.debug(f"Dati preparati: {json.dumps(final_data)}")

                producer.send(KAFKA_TOPIC, value=final_data)
                producer.flush()
                logger.success(f"Dati inviati con successo a Kafka.")

            else:
                logger.warning("Nessun dato valido ricevuto dalla boa. Utilizzo solo dati simulati.")
                
                # Crea dati completamente simulati
                simulated_data = {
                    'sensor_id': BUOY_ID,
                    'LAT': 42.345,
                    'LON': -68.502,
                    'WDIR': random.randint(0, 359),
                    'WSPD': round(random.uniform(0, 20), 1),
                    'GST': round(random.uniform(0, 25), 1),
                    'WVHT': round(random.uniform(0, 5), 2),
                    'DPD': round(random.uniform(5, 15), 1),
                    'WTMP': round(random.uniform(10, 25), 1),
                    'timestamp': int(time.time() * 1000)
                }
                
                final_data = simulate_water_quality_metrics(simulated_data)
                
                logger.debug(f"Dati simulati: {json.dumps(final_data)}")
                
                producer.send(KAFKA_TOPIC, value=final_data)
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
