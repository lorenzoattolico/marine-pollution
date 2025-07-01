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
INPUT_TOPIC = "water_metrics_raw"
OUTPUT_TOPIC = "gold_water_data"
ALERTS_TOPIC = "sensor_alerts"

def main():
    # Configurazione dell'ambiente di esecuzione
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Impostato a 1 per semplicità, aumentare in produzione
    
    # Registrazione dei jar necessari per Kafka
    env.add_jars("file:///opt/flink/usrlib/flink-connector-kafka-1.17.0.jar",
                "file:///opt/flink/usrlib/kafka-clients-3.3.2.jar")
    
    # Configurazione source Kafka per i dati delle stazioni
    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_topics(INPUT_TOPIC) \
        .set_group_id("water-metrics-flink-job") \
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
    water_metrics_stream = env.from_source(source, watermark_strategy=None, source_name="Kafka Source")
    
    # Trasforma e processa i dati delle stazioni
    processed_stream = water_metrics_stream.map(process_water_metrics, output_type=Types.STRING())
    
    # Flusso separato per il rilevamento delle anomalie
    anomaly_stream = water_metrics_stream.map(detect_anomalies, output_type=Types.STRING()) \
        .filter(lambda x: x is not None)
    
    # Invio dei dati elaborati al sink
    processed_stream.sink_to(sink)
    
    # Invio degli alert al sink dedicato
    anomaly_stream.sink_to(alert_sink)
    
    # Esecuzione del job
    env.execute("Water Metrics Processing Job")

def process_water_metrics(json_data):
    """
    Elabora i dati delle stazioni di qualità dell'acqua: normalizza, filtra e arricchisce i dati.
    """
    try:
        # Parsing del JSON
        data = json.loads(json_data)
        
        # Normalizzazione e arricchimento dei dati
        processed_data = {
            "sensor_id": data.get("sensor_id", "unknown"),
            "timestamp": data.get("timestamp", datetime.now().isoformat()),
            "lat": data.get("lat", 0),
            "lon": data.get("lon", 0),
            "measurements": {},
            "derived_metrics": {}
        }
        
        # Copia le misurazioni originali
        measurements = ["pH", "turbidity", "temperature_c", "dissolved_oxygen", "conductivity", "nitrate"]
        for metric in measurements:
            if metric in data:
                processed_data["measurements"][metric] = data[metric]
        
        # Calcola metriche derivate
        
        # Indice di qualità dell'acqua (WQI) semplificato
        # Formula basata su pH, torbidità e ossigeno disciolto
        if all(metric in data for metric in ["pH", "turbidity", "dissolved_oxygen"]):
            # Normalizzazione dei parametri
            ph = data["pH"]
            ph_score = 100 if 6.5 <= ph <= 8.5 else max(0, 100 - 20 * min(abs(ph - 7.5), 5))
            
            turbidity = data["turbidity"]
            turbidity_score = max(0, 100 - (turbidity * 10)) if turbidity <= 10 else 0
            
            do = data["dissolved_oxygen"]
            do_score = min(100, do * 10) if do <= 10 else 100
            
            # Calcolo WQI
            wqi = (ph_score * 0.3) + (turbidity_score * 0.3) + (do_score * 0.4)
            processed_data["derived_metrics"]["water_quality_index"] = round(wqi, 1)
            
            # Classificazione della qualità dell'acqua
            if wqi >= 80:
                processed_data["derived_metrics"]["water_quality"] = "excellent"
            elif wqi >= 60:
                processed_data["derived_metrics"]["water_quality"] = "good"
            elif wqi >= 40:
                processed_data["derived_metrics"]["water_quality"] = "fair"
            else:
                processed_data["derived_metrics"]["water_quality"] = "poor"
        
        # Indice di inquinamento da nitrati
        if "nitrate" in data:
            nitrate = data["nitrate"]
            if nitrate < 1.0:
                processed_data["derived_metrics"]["nitrate_pollution"] = "minimal"
            elif nitrate < 3.0:
                processed_data["derived_metrics"]["nitrate_pollution"] = "moderate"
            else:
                processed_data["derived_metrics"]["nitrate_pollution"] = "significant"
        
        # Aggiunge timestamp di elaborazione
        processed_data["processing_timestamp"] = int(time.time() * 1000)
        
        return json.dumps(processed_data)
    
    except Exception as e:
        print(f"Error processing water metrics data: {e}")
        return json.dumps({"error": str(e), "raw_data": json_data})

def detect_anomalies(json_data):
    """
    Rileva anomalie nei dati delle stazioni di qualità dell'acqua.
    """
    try:
        data = json.loads(json_data)
        
        # Controlli di anomalia per pH
        if "pH" in data:
            ph = float(data["pH"])
            if ph < 6.0 or ph > 9.0:
                alert = {
                    "sensor_id": data.get("sensor_id", "unknown"),
                    "timestamp": data.get("timestamp", datetime.now().isoformat()),
                    "lat": data.get("lat", 0),
                    "lon": data.get("lon", 0),
                    "alert_type": "pH_anomaly",
                    "value": ph,
                    "threshold_min": 6.0,
                    "threshold_max": 9.0,
                    "severity": "high" if (ph < 5.0 or ph > 10.0) else "medium",
                    "description": f"Abnormal pH level detected in water station: {ph}",
                    "alert_timestamp": int(time.time() * 1000)
                }
                return json.dumps(alert)
        
        # Controlli di anomalia per torbidità
        if "turbidity" in data:
            turbidity = float(data["turbidity"])
            if turbidity > 5.0:
                alert = {
                    "sensor_id": data.get("sensor_id", "unknown"),
                    "timestamp": data.get("timestamp", datetime.now().isoformat()),
                    "lat": data.get("lat", 0),
                    "lon": data.get("lon", 0),
                    "alert_type": "turbidity_anomaly",
                    "value": turbidity,
                    "threshold": 5.0,
                    "severity": "high" if turbidity > 10.0 else "medium",
                    "description": f"High turbidity level detected in water station: {turbidity}",
                    "alert_timestamp": int(time.time() * 1000)
                }
                return json.dumps(alert)
        
        # Controlli di anomalia per nitrati
        if "nitrate" in data:
            nitrate = float(data["nitrate"])
            if nitrate > 3.0:
                alert = {
                    "sensor_id": data.get("sensor_id", "unknown"),
                    "timestamp": data.get("timestamp", datetime.now().isoformat()),
                    "lat": data.get("lat", 0),
                    "lon": data.get("lon", 0),
                    "alert_type": "nitrate_anomaly",
                    "value": nitrate,
                    "threshold": 3.0,
                    "severity": "high" if nitrate > 5.0 else "medium",
                    "description": f"High nitrate level detected in water station: {nitrate} mg/L",
                    "alert_timestamp": int(time.time() * 1000)
                }
                return json.dumps(alert)
        
        # Controlli di anomalia per ossigeno disciolto
        if "dissolved_oxygen" in data:
            do = float(data["dissolved_oxygen"])
            if do < 5.0:
                alert = {
                    "sensor_id": data.get("sensor_id", "unknown"),
                    "timestamp": data.get("timestamp", datetime.now().isoformat()),
                    "lat": data.get("lat", 0),
                    "lon": data.get("lon", 0),
                    "alert_type": "low_oxygen_anomaly",
                    "value": do,
                    "threshold": 5.0,
                    "severity": "high" if do < 3.0 else "medium",
                    "description": f"Low dissolved oxygen level detected in water station: {do} mg/L",
                    "alert_timestamp": int(time.time() * 1000)
                }
                return json.dumps(alert)
        
        # Nessuna anomalia rilevata
        return None
    
    except Exception as e:
        print(f"Error detecting water metrics anomalies: {e}")
        return None

if __name__ == "__main__":
    main()