import os
import json
import time
import boto3
from datetime import datetime
from botocore.client import Config
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.kafka import KafkaOffsetResetStrategy

# Configurazione
KAFKA_BROKERS = os.environ.get('KAFKA_BROKER', 'kafka:29092')
INPUT_TOPIC = "satellite_imagery_raw"
OUTPUT_TOPIC = "gold_satellite_data"
ALERTS_TOPIC = "sensor_alerts"

# Configurazione MinIO
MINIO_ENDPOINT = f"http://{os.environ.get('MINIO_HOST', 'minio')}:{os.environ.get('MINIO_PORT', '9000')}"
MINIO_ACCESS_KEY = os.environ.get('MINIO_ROOT_USER', 'minioadmin')
MINIO_SECRET_KEY = os.environ.get('MINIO_ROOT_PASSWORD', 'minioadmin')
MINIO_BRONZE_BUCKET = "marine-bronze"
MINIO_SILVER_BUCKET = "marine-silver"

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
        
        # Verifica se i bucket esistono, se no li crea
        for bucket in [MINIO_BRONZE_BUCKET, MINIO_SILVER_BUCKET]:
            try:
                client.head_bucket(Bucket=bucket)
            except:
                client.create_bucket(Bucket=bucket)
                print(f"Bucket {bucket} creato.")
        
        return client
    except Exception as e:
        print(f"Errore nella connessione a MinIO: {e}")
        return None

def main():
    # Configurazione dell'ambiente di esecuzione
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Impostato a 1 per semplicità, aumentare in produzione
    
    # Registrazione dei jar necessari per Kafka
    env.add_jars("file:///opt/flink/usrlib/flink-connector-kafka-1.17.0.jar",
                "file:///opt/flink/usrlib/kafka-clients-3.3.2.jar")
    
    # Configurazione source Kafka per i dati satellitari
    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_topics(INPUT_TOPIC) \
        .set_group_id("satellite-flink-job") \
        .set_properties({'auto.offset.reset': 'latest'}) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    # Configurazione sink Kafka per i dati elaborati
    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(OUTPUT_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
    
    # Configurazione sink Kafka per gli alert
    alert_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(ALERTS_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
    
    # Creazione del data stream e definizione della logica di elaborazione
    satellite_stream = env.from_source(source, watermark_strategy=None, source_name="Kafka Source")
    
    # Trasforma e processa i dati satellitari
    processed_stream = satellite_stream.map(process_satellite_data, output_type=Types.STRING())
    
    # Flusso separato per il rilevamento delle anomalie
    anomaly_stream = satellite_stream.map(detect_anomalies, output_type=Types.STRING()) \
        .filter(lambda x: x is not None)
    
    # Invio dei dati elaborati al sink
    processed_stream.sink_to(sink)
    
    # Invio degli alert al sink dedicato
    anomaly_stream.sink_to(alert_sink)
    
    # Esecuzione del job
    env.execute("Satellite Data Processing Job")

def process_satellite_data(json_data):
    """
    Elabora i dati satellitari: legge i metadati da MinIO, calcola indici di inquinamento,
    salva i risultati nel bucket silver e invia i metadati elaborati a Kafka.
    """
    try:
        # Parsing del JSON
        data = json.loads(json_data)
        
        # Ottiene il client MinIO
        minio_client = get_minio_client()
        if not minio_client:
            raise Exception("Failed to connect to MinIO")
        
        # Legge i metadati completi da MinIO
        storage_path = data.get("storage_path")
        if not storage_path:
            raise Exception("Missing storage_path in metadata")
        
        response = minio_client.get_object(
            Bucket=MINIO_BRONZE_BUCKET,
            Key=storage_path
        )
        
        full_metadata = json.loads(response['Body'].read().decode('utf-8'))
        
        # Elabora gli indici di inquinamento
        processed_data = {
            "image_id": full_metadata.get("image_id"),
            "timestamp": full_metadata.get("timestamp"),
            "lat": full_metadata.get("lat"),
            "lon": full_metadata.get("lon"),
            "area_name": full_metadata.get("area_name"),
            "indices": full_metadata.get("indices", {}),
            "processed_indices": {}
        }
        
        # Calcola indici aggiuntivi o rielabora gli indici esistenti
        # Qui puoi aggiungere algoritmi di image processing più complessi
        
        # Esempio: calcola Normalized Difference Turbidity Index (NDTI) modificato
        if "B3" in full_metadata.get("bands", {}) and "B8" in full_metadata.get("bands", {}):
            green = full_metadata["bands"]["B3"]
            nir = full_metadata["bands"]["B8"]
            if nir + green > 0:
                modified_ndti = (nir - green) / (nir + green)
                processed_data["processed_indices"]["modified_ndti"] = round(modified_ndti, 4)
        
        # Esempio: calcola indice di inquinamento marino sintetico
        if "NDWI" in full_metadata.get("indices", {}) and "NDTI" in full_metadata.get("indices", {}):
            ndwi = full_metadata["indices"]["NDWI"]
            ndti = full_metadata["indices"]["NDTI"]
            # Formula sintetica di esempio
            marine_pollution_index = (0.7 * (1 - ndwi) + 0.3 * ndti)
            processed_data["processed_indices"]["marine_pollution_index"] = round(marine_pollution_index, 4)
            
            # Classificazione dell'inquinamento
            if marine_pollution_index < 0.3:
                pollution_level = "low"
            elif marine_pollution_index < 0.6:
                pollution_level = "medium"
            else:
                pollution_level = "high"
            
            processed_data["pollution_level"] = pollution_level
        
        # Salva i dati elaborati nel bucket silver
        silver_path = f"processed/{processed_data['image_id']}.json"
        minio_client.put_object(
            Bucket=MINIO_SILVER_BUCKET,
            Key=silver_path,
            Body=json.dumps(processed_data).encode('utf-8'),
            ContentType='application/json'
        )
        
        # Aggiungi il percorso del file elaborato
        processed_data["processed_storage_path"] = silver_path
        processed_data["processing_timestamp"] = int(time.time() * 1000)
        
        return json.dumps(processed_data)
    
    except Exception as e:
        print(f"Error processing satellite data: {e}")
        return json.dumps({"error": str(e), "raw_data": json_data})

def detect_anomalies(json_data):
    """
    Rileva anomalie nei dati satellitari.
    """
    try:
        # Parsing del JSON
        data = json.loads(json_data)
        
        # Ottiene il client MinIO
        minio_client = get_minio_client()
        if not minio_client:
            return None
        
        # Legge i metadati completi da MinIO
        storage_path = data.get("storage_path")
        if not storage_path:
            return None
        
        try:
            response = minio_client.get_object(
                Bucket=MINIO_BRONZE_BUCKET,
                Key=storage_path
            )
            
            full_metadata = json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            print(f"Failed to read metadata from MinIO: {e}")
            return None
        
        # Controlli di anomalia per gli indici
        if "indices" in full_metadata:
            indices = full_metadata["indices"]
            
            # Controlla NDTI (torbidità)
            if "NDTI" in indices:
                ndti = float(indices["NDTI"])
                if ndti > 0.3:  # Soglia arbitraria per alta torbidità
                    alert = {
                        "image_id": full_metadata.get("image_id"),
                        "timestamp": full_metadata.get("timestamp"),
                        "lat": full_metadata.get("lat"),
                        "lon": full_metadata.get("lon"),
                        "alert_type": "high_turbidity_detected",
                        "value": ndti,
                        "threshold": 0.3,
                        "severity": "high" if ndti > 0.4 else "medium",
                        "description": f"High turbidity detected in satellite imagery: NDTI = {ndti}",
                        "alert_timestamp": int(time.time() * 1000)
                    }
                    return json.dumps(alert)
            
            # Controlla NDWI (indice di acqua) per potenziali inquinanti che alterano la riflettanza dell'acqua
            if "NDWI" in indices:
                ndwi = float(indices["NDWI"])
                if ndwi < -0.1:  # Soglia arbitraria per anomalia NDWI
                    alert = {
                        "image_id": full_metadata.get("image_id"),
                        "timestamp": full_metadata.get("timestamp"),
                        "lat": full_metadata.get("lat"),
                        "lon": full_metadata.get("lon"),
                        "alert_type": "water_anomaly_detected",
                        "value": ndwi,
                        "threshold": -0.1,
                        "severity": "high" if ndwi < -0.2 else "medium",
                        "description": f"Abnormal water signature detected in satellite imagery: NDWI = {ndwi}",
                        "alert_timestamp": int(time.time() * 1000)
                    }
                    return json.dumps(alert)
        
        # Nessuna anomalia rilevata
        return None
    
    except Exception as e:
        print(f"Error detecting satellite anomalies: {e}")
        return None

if __name__ == "__main__":
    main()