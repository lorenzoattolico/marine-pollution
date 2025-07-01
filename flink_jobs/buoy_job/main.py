import os
import json
import time
from datetime import datetime
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.kafka import KafkaOffsetResetStrategy

# Configurazione
KAFKA_BROKERS = os.environ.get('KAFKA_BROKER', 'kafka:29092')
INPUT_TOPIC = "buoy_raw"
OUTPUT_TOPIC = "gold_buoy_data"
ALERTS_TOPIC = "sensor_alerts"

def main():
    # Configurazione dell'ambiente di esecuzione
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Impostato a 1 per semplicità, aumentare in produzione
    
    # Registrazione dei jar necessari per Kafka
    env.add_jars("file:///opt/flink/usrlib/flink-connector-kafka-1.17.0.jar",
                "file:///opt/flink/usrlib/kafka-clients-3.3.2.jar")
    
    # Configurazione source Kafka per i dati delle boe
    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_topics(INPUT_TOPIC) \
        .set_group_id("buoy-flink-job") \
        .set_starting_offsets(KafkaOffsetResetStrategy.LATEST) \
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
    buoy_stream = env.from_source(source, watermark_strategy=None, source_name="Kafka Source")
    
    # Trasforma e processa i dati delle boe
    processed_stream = buoy_stream.map(process_buoy_data, output_type=Types.STRING())
    
    # Flusso separato per il rilevamento delle anomalie
    anomaly_stream = buoy_stream.map(detect_anomalies, output_type=Types.STRING()) \
        .filter(lambda x: x is not None)
    
    # Invio dei dati elaborati al sink
    processed_stream.sink_to(sink)
    
    # Invio degli alert al sink dedicato
    anomaly_stream.sink_to(alert_sink)
    
    # Esecuzione del job
    env.execute("Buoy Data Processing Job")

def process_buoy_data(json_data):
    """
    Elabora i dati delle boe: normalizza, filtra e arricchisce i dati.
    """
    try:
        # Parsing del JSON
        data = json.loads(json_data)
        
        # Normalizzazione delle chiavi
        normalized_data = {
            "sensor_id": data.get("sensor_id", "unknown"),
            "timestamp": data.get("timestamp", int(time.time() * 1000)),
            "lat": float(data.get("LAT", 0)),
            "lon": float(data.get("LON", 0)),
            "measurements": {}
        }
        
        # Aggiungi le misurazioni normalizzate
        for key, value in data.items():
            # Ignora chiavi già processate e non numeriche
            if key not in ["sensor_id", "timestamp", "LAT", "LON"] and isinstance(value, (int, float)):
                normalized_data["measurements"][key] = value
        
        # Calcola metriche derivate
        if "pH" in data and "turbidity" in data:
            # Esempio di metrica derivata: water quality index (WQI)
            # Formula semplificata: WQI = (pH_normalized * 0.4 + turbidity_normalized * 0.6)
            ph_normalized = max(0, min(1, (data["pH"] - 6.5) / 2.0)) if 6.5 <= data["pH"] <= 8.5 else 0
            turbidity_normalized = max(0, min(1, 1.0 - (data["turbidity"] / 5.0)))
            normalized_data["measurements"]["water_quality_index"] = round(ph_normalized * 0.4 + turbidity_normalized * 0.6, 2)
        
        # Aggiunge timestamp di elaborazione
        normalized_data["processing_timestamp"] = int(time.time() * 1000)
        
        return json.dumps(normalized_data)
    
    except Exception as e:
        print(f"Error processing buoy data: {e}")
        return json.dumps({"error": str(e), "raw_data": json_data})

def detect_anomalies(json_data):
    """
    Rileva anomalie nei dati delle boe.
    """
    try:
        data = json.loads(json_data)
        
        # Controlli di anomalia per pH
        if "pH" in data:
            ph = float(data["pH"])
            if ph < 6.0 or ph > 9.0:
                alert = {
                    "sensor_id": data.get("sensor_id", "unknown"),
                    "timestamp": data.get("timestamp", int(time.time() * 1000)),
                    "lat": float(data.get("LAT", 0)),
                    "lon": float(data.get("LON", 0)),
                    "alert_type": "pH_anomaly",
                    "value": ph,
                    "threshold_min": 6.0,
                    "threshold_max": 9.0,
                    "severity": "high" if (ph < 5.5 or ph > 9.5) else "medium",
                    "description": f"Abnormal pH level detected: {ph}",
                    "alert_timestamp": int(time.time() * 1000)
                }
                return json.dumps(alert)
        
        # Controlli di anomalia per torbidità
        if "turbidity" in data:
            turbidity = float(data["turbidity"])
            if turbidity > 5.0:
                alert = {
                    "sensor_id": data.get("sensor_id", "unknown"),
                    "timestamp": data.get("timestamp", int(time.time() * 1000)),
                    "lat": float(data.get("LAT", 0)),
                    "lon": float(data.get("LON", 0)),
                    "alert_type": "turbidity_anomaly",
                    "value": turbidity,
                    "threshold": 5.0,
                    "severity": "high" if turbidity > 8.0 else "medium",
                    "description": f"High turbidity level detected: {turbidity}",
                    "alert_timestamp": int(time.time() * 1000)
                }
                return json.dumps(alert)
        
        # Controlli di anomalia per contaminanti
        if "contaminant_ppm" in data:
            contaminant = float(data["contaminant_ppm"])
            if contaminant > 7.0:
                alert = {
                    "sensor_id": data.get("sensor_id", "unknown"),
                    "timestamp": data.get("timestamp", int(time.time() * 1000)),
                    "lat": float(data.get("LAT", 0)),
                    "lon": float(data.get("LON", 0)),
                    "alert_type": "contaminant_anomaly",
                    "value": contaminant,
                    "threshold": 7.0,
                    "severity": "high" if contaminant > 10.0 else "medium",
                    "description": f"High contaminant level detected: {contaminant} ppm",
                    "alert_timestamp": int(time.time() * 1000)
                }
                return json.dumps(alert)
        
        # Nessuna anomalia rilevata
        return None
    
    except Exception as e:
        print(f"Error detecting anomalies: {e}")
        return None

if __name__ == "__main__":
    main()