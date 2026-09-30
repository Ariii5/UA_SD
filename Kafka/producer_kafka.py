from kafka import KafkaProducer
from json import dumps
from time import sleep

# Conectamos el productor a nuestro Kafka local y le enseñamos a empaquetar los mensajes
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

print("Empezando a enviar 5 mensajes de prueba...")

for e in range(5):
    datos = {'numero_prueba': e, 'texto': 'Hola Caracola'}
    
    # Enviar el mensaje al topic 'numtest'
    producer.send('numtest', value=datos)
    print(f"Mensaje enviado: {datos}")
    
    sleep(2) # Espera 2 segundos antes del siguiente

# Asegurarse de que todo se ha enviado bien
producer.flush()
print("Todos los mensajes enviados.")