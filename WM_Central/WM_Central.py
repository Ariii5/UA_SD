import sys
import socket
import threading
from db_manager import init_db, get_estaciones

ENQ = b'\x05'   # Enquiry: solicitud de inicio
ACK = b'\x06'   # Acknowledge: confirmación OK
NACK = b'\x15'  # Negative Acknowledge: error / no reconocido

def handle_monitor_client(client_socket, client_address):
    """Atiende a un monitor (WM_WS_M) en un hilo independiente."""
    print(f"\n[CENTRAL] Conexión TCP entrante desde {client_address}")
    try:
        data = client_socket.recv(1024)
        
        if data == ENQ:
            print(f"[CENTRAL] <ENQ> recibido de {client_address}. Enviando <ACK>...")
            client_socket.sendall(ACK)
            print(f"[CENTRAL] <ACK> enviado. Canal de comunicación establecido con éxito.")
        else:
            print(f"[CENTRAL] Byte recibido desconocido ({data}). Enviando <NACK>...")
            client_socket.sendall(NACK)
            
    except Exception as e:
        print(f"[CENTRAL] Error con el cliente {client_address}: {e}")
    finally:
        client_socket.close()
        print(f"[CENTRAL] Conexión de prueba cerrada con {client_address}.\n")

def start_socket_server(port):
    """Crea y levanta el socket servidor TCP."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    server.bind(("0.0.0.0", port))
    server.listen(5)
    print(f"[CENTRAL] Servidor de Sockets a la escucha en el puerto {port}...")
    
    while True:
        client_sock, addr = server.accept()
        thread = threading.Thread(target=handle_monitor_client, args=(client_sock, addr))
        thread.daemon = True
        thread.start()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python3 WM_Central.py <puerto_sockets> <ip_kafka:puerto>")
        print("Ejemplo: python3 WM_Central.py 8000 localhost:9092")
        sys.exit(1)
        
    socket_port = int(sys.argv[1])
    kafka_server = sys.argv[2]
    
    print("=" * 50)
    print("      INICIANDO SISTEMA WM_CENTRAL")
    print("=" * 50)

    init_db()
    print("\n--- Estado inicial de estaciones (BD) ---")
    for est in get_estaciones():
        print(f"[{est[2]}] ID: {est[0]} | Zona: {est[1]}")
    print("-" * 40)

    start_socket_server(socket_port)