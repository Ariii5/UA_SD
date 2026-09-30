from kafka import KafkaConsumer
from json import loads

# Creamos el consumidor, le decimos que mire 'numtest' y se conecte a tu Kafka local
consumer = KafkaConsumer(
    'numtest',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='mi-grupo-prueba',
    value_deserializer=lambda x: loads(x.decode('utf-8')) # Traduce el mensaje recibido
)

print("Consumidor encendido. Esperando a que el productor envíe algo...")

# Se queda en bucle infinito escuchando el tablón
for message in consumer:
    print(f'¡Mensaje recibido!: {message.value}')