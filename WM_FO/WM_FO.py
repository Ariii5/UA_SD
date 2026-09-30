import argparse
from kafka import KafkaProducer

def iniciar_operario():
    # 1. Preparar el programa para leer lo que escribimos en la terminal
    parser = argparse.ArgumentParser(description="Aplicación del Operario (WM_FO)")
    
    # 2. Definir qué datos obligatorios le vamos a pedir
    parser.add_argument('--broker', required=True, help='IP y puerto de Kafka (ej: localhost:9092)')
    parser.add_argument('--id', required=True, help='ID único del operario (ej: OP-01)')
    
    # 3. Leer los datos ingresados
    args = parser.parse_args()
    
    broker = args.broker
    id_operario = args.id
    
    # 4. Imprimir por pantalla para comprobar que los ha leído bien
    print("=======================================")
    print(f"🚜 INICIANDO TERMINAL DE OPERARIO")
    print(f"👤 ID del Operario: {id_operario}")
    print(f"📡 Conectando al broker: {broker}")
    print("=======================================\n")
    
    # Aquí es donde en la Semana 2 meteremos la lógica para enviar peticiones de riego
    print("Esperando órdenes...")

if __name__ == "__main__":
    iniciar_operario()