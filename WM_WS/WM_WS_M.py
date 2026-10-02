import sys
import socket
import threading

# Constantes del protocolo ASCII sugerido en el enunciado
ENQ = b'\x05'   # Solicitud inicial
ACK = b'\x06'   # Confirmación positiva
NACK = b'\x15'  # Confirmación negativa

# ==========================================
# ROL SERVIDOR: Escuchar al Engine (WM_WS_E)
# ==========================================
def handle_engine_connection(engine_socket, engine_address, ws_id):
    """Atiende los mensajes y eventos que envíe el Engine local."""
    print(f"[{ws_id} - SERVIDOR] Engine conectado desde {engine_address}")
    try:
        while True:
            data = engine_socket.recv(1024)
            if not data:
                break
            print(f"[{ws_id} - SERVIDOR] Mensaje recibido del Engine: {data}")
    except Exception as e:
        print(f"[{ws_id} - SERVIDOR] Conexión con Engine interrumpida: {e}")
    finally:
        engine_socket.close()
        print(f"[{ws_id} - SERVIDOR] Engine desconectado.")

def start_engine_server(listen_port, ws_id):
    """Servidor TCP en segundo plano a la espera del Engine."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", listen_port))
    server.listen(1)
    print(f"[{ws_id} - SERVIDOR] Esperando conexión local del Engine en el puerto {listen_port}...")
    
    while True:
        engine_sock, addr = server.accept()
        t = threading.Thread(target=handle_engine_connection, args=(engine_sock, addr, ws_id))
        t.daemon = True
        t.start()

# ==========================================
# ROL CLIENTE: Conectar con WM_Central
# ==========================================
def connect_to_central(central_ip, central_port, ws_id):
    """Se conecta como cliente a WM_Central y realiza el apretón de manos inicial."""
    print(f"[{ws_id} - CLIENTE] Conectando con Central en {central_ip}:{central_port}...")
    try:
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((central_ip, central_port))
        print(f"[{ws_id} - CLIENTE] Conexión física establecida. Enviando <ENQ>...")

        # Envío del byte inicial <ENQ>
        client_sock.sendall(ENQ)

        # Espera de la respuesta <ACK>
        response = client_sock.recv(1024)
        if response == ACK:
            print(f"[{ws_id} - CLIENTE] ¡<ACK> recibido de WM_Central! Enlace confirmado.")
        else:
            print(f"[{ws_id} - CLIENTE] Respuesta no esperada de Central: {response}")

        client_sock.close()
        print(f"[{ws_id} - CLIENTE] Canal de prueba con Central cerrado con éxito.")
    except ConnectionRefusedError:
        print(f"[{ws_id} - CLIENTE] Error: No se pudo conectar a Central. Comprueba que WM_Central.py esté ejecutándose.")
    except Exception as e:
        print(f"[{ws_id} - CLIENTE] Error en la comunicación con Central: {e}")

# ==========================================
# PUNTO DE ENTRADA
# ==========================================
if __name__ == "__main__":
    # IP Engine, 2: Puerto Engine, 3: Puerto servidor local, 4: IP Central, 5: Puerto Central, 6: ID WS
    if len(sys.argv) < 7:
        print("Uso: python3 WM_WS_M.py <ip_e> <puerto_e> <puerto_escucha_e> <ip_central> <puerto_central> <id_ws>")
        print("Ejemplo: python3 WM_WS_M.py 127.0.0.1 6001 5001 127.0.0.1 8000 WS-01")
        sys.exit(1)

    ip_engine = sys.argv[1]
    puerto_engine = int(sys.argv[2])
    puerto_escucha_engine = int(sys.argv[3])
    ip_central = sys.argv[4]
    puerto_central = int(sys.argv[5])
    ws_id = sys.argv[6]

    print("=" * 55)
    print(f"   INICIANDO MONITOR ({ws_id}) [CLIENTE & SERVIDOR]")
    print("=" * 55)

    # Arrancar el servidor para el Engine en segundo plano (demonio)
    engine_server_thread = threading.Thread(
        target=start_engine_server, 
        args=(puerto_escucha_engine, ws_id),
        daemon=True
    )
    engine_server_thread.start()

    # Conectar a Central como cliente en el hilo principal
    connect_to_central(ip_central, puerto_central, ws_id)

    # Mantener el monitor activo para que el servidor local de sockets no muera
    try:
        engine_server_thread.join()
    except KeyboardInterrupt:
        print(f"\n[{ws_id}] Monitor detenido manualmente.")