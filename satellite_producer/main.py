import time
import json
import sys
import os
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from loguru import logger
import boto3
from botocore.client import Config

# Carica variabili d'ambiente
load_dotenv()

# Configurazione
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
KAFKA_TOPIC = 'satellite_imagery_raw'
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL', 600))  # 10 minuti di default

# Configurazione MinIO
MINIO_ENDPOINT = f"http://{os.getenv('MINIO_HOST', 'minio')}:{os.getenv('MINIO_PORT', '9000')}"
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')
MINIO_BUCKET = 'marine-bronze'

# Configurazione logger
logger.remove()
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")
logger.add("satellite_producer.log", rotation="10 MB", level="DEBUG")

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

def get_minio_client():
    """Crea e restituisce un client MinIO."""
    try:
        client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'  # Fake region
        )
        # Verifica se il bucket esiste, se no lo crea
        try:
            client.head_bucket(Bucket=MINIO_BUCKET)
        except:
            client.create_bucket(Bucket=MINIO_BUCKET)
            logger.info(f"Bucket {MINIO_BUCKET} creato.")
        
        return client
    except Exception as e:
        logger.error(f"Errore nella connessione a MinIO: {e}")
        return None

def generate_satellite_data():
    """
    In un ambiente reale, questa funzione scaricherebbe immagini 
    dalla Sentinel Hub API. Per semplicità, generiamo dati simulati.
    """
    # Definisci alcune aree marine di interesse
    areas = [
        {"name": "North Atlantic", "lat_range": (30, 45), "lon_range": (-75, -50)},
        {"name": "Mediterranean", "lat_range": (35, 45), "lon_range": (5, 25)},
        {"name": "Gulf of Mexico", "lat_range": (20, 30), "lon_range": (-98, -80)},
        {"name": "South Pacific", "lat_range": (-30, -10), "lon_range": (150, 180)}
    ]
    
    area = random.choice(areas)
    lat = random.uniform(*area["lat_range"])
    lon = random.uniform(*area["lon_range"])
    
    # Genera un ID univoco per l'immagine
    image_id = str(uuid.uuid4())
    
    # Prepara i metadati
    metadata = {
        'image_id': image_id,
        'timestamp': datetime.now().isoformat(),
        'lat': lat,
        'lon': lon,
        'area_name': area["name"],
        'cloud_coverage': round(random.uniform(0, 100), 2),
        'resolution': "10m",
        'source': 'satellite_simulation',
        'bands': {
            'B2': round(random.uniform(0.05, 0.2), 3),  # Blue
            'B3': round(random.uniform(0.05, 0.2), 3),  # Green
            'B4': round(random.uniform(0.05, 0.3), 3),  # Red
            'B8': round(random.uniform(0.1, 0.5), 3),   # NIR
            'B11': round(random.uniform(0.1, 0.5), 3),  # SWIR
            'B12': round(random.uniform(0.1, 0.5), 3),  # SWIR2
        },
        'indices': {
            'NDVI': round(random.uniform(-0.1, 0.8), 3),  # Indice di vegetazione
            'NDWI': round(random.uniform(-0.3, 0.5), 3),  # Indice di acqua
            'NDTI': round(random.uniform(0, 0.5), 3),     # Indice di torbidità
        },
        'storage_path': f"satellite/{image_id}.json"
    }
    
    return metadata

def main():
    """Ciclo principale del producer."""
    producer = create_kafka_producer()
    minio_client = get_minio_client()
    
    if not minio_client:
        logger.critical("Impossibile connettersi a MinIO. Uscita.")
        sys.exit(1)
    
    logger.info(f"Producer satellitare avviato. Invio dati al topic '{KAFKA_TOPIC}'.")
    
    while True:
        try:
            # Genera metadati satellitari
            satellite_data = generate_satellite_data()
            
            # Salva i metadati completi su MinIO
            try:
                minio_client.put_object(
                    Bucket=MINIO_BUCKET,
                    Key=satellite_data['storage_path'],
                    Body=json.dumps(satellite_data).encode('utf-8'),
                    ContentType='application/json'
                )
                logger.info(f"Metadati salvati su MinIO: {satellite_data['storage_path']}")
            except Exception as e:
                logger.error(f"Errore nel salvataggio su MinIO: {e}")
            
            # Invia riferimento ai metadati su Kafka
            kafka_payload = {
                'image_id': satellite_data['image_id'],
                'timestamp': satellite_data['timestamp'],
                'lat': satellite_data['lat'],
                'lon': satellite_data['lon'],
                'area_name': satellite_data['area_name'],
                'storage_path': satellite_data['storage_path']
            }
            
            logger.info(f"Invio dati satellitari: {satellite_data['image_id']} - Area: {satellite_data['area_name']}")
            producer.send(KAFKA_TOPIC, value=kafka_payload)
            producer.flush()
            logger.success(f"Metadati dell'immagine inviati con successo a Kafka.")
            
            logger.info(f"Prossimo controllo tra {POLL_INTERVAL_SECONDS // 60} minuti...")
            time.sleep(POLL_INTERVAL_SECONDS)
            
        except KeyboardInterrupt:
            logger.info("Rilevato arresto manuale. Arrivederci!")
            break
        except Exception as e:
            logger.error(f"Errore inaspettato nel ciclo principale: {e}")
            time.sleep(60)

if __name__ == '__main__':
    main()
