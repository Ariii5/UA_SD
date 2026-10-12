# Herramienta de prueba: muestra en pantalla todo lo que llega a un topic de Kafka.
# Uso: py Kafka\ver_topic.py <topic> [ip_broker:puerto]
# Ej.: py Kafka\ver_topic.py wm_telemetria
import sys
import json
from kafka import KafkaConsumer

topic = sys.argv[1] if len(sys.argv) > 1 else "wm_telemetria"
broker = sys.argv[2] if len(sys.argv) > 2 else "localhost:9092"

consumer = KafkaConsumer(
    bootstrap_servers=[broker],
    auto_offset_reset="latest",     # solo mensajes nuevos, no el historial
    metadata_max_age_ms=3000,       # pregunta a Kafka por topics nuevos cada 3 s (por defecto 5 min)
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)
consumer.subscribe([topic])
print(f"Conectando al topic '{topic}' en {broker}...")

# Esperamos a que Kafka nos asigne la partición: hasta entonces no recibimos nada
while not consumer.assignment():
    consumer.poll(timeout_ms=500)
print(">>> LISTO. Ya puedes generar mensajes. (Ctrl+C para salir)")

try:
    while True:
        lotes = consumer.poll(timeout_ms=500)
        for registros in lotes.values():
            for msg in registros:
                print(msg.value)
except KeyboardInterrupt:
    pass