import sys
import socket
import threading
import time
#su mision es revisar WM_WS_E y avisar a la central si detecta un fallo
# ==============================================================================
# BYTES DE CONTROL (El protocolo de saludo)
# ==============================================================================
ENQ = b'\x05'   # "¿Estás ahí?" (Enquiry). Lo enviamos nosotros a Central.
ACK = b'\x06'   # "Sí, todo bien" (Acknowledge). Esperamos que Central nos conteste esto.
NACK = b'\x15'  # "Algo fue mal" (Negative Acknowledge).

# Variables globales para compartir los sockets entre hilos
sock_central_global = None

# ==============================================================================
# ROL 1: SERVIDOR (El Monitor escucha y vigila a su propio motor local WM_WS_E)
# ==============================================================================
def health_pulse_worker(engine_socket, ws_id):
    """
    Hilo dedicado a vigilar la salud del Engine cada segundo.
    Si el Engine responde KO, notifica de inmediato a Central vía Socket.
    """
    global sock_central_global
    print(f"[{ws_id} - SALUD] Iniciando monitoreo periódico cada 1s con el Engine...")
    
    while True:
        time.sleep(1)
        try:
            # Enviamos la pregunta de salud al motor
            engine_socket.sendall(b"HEALTH?")
            respuesta = engine_socket.recv(1024) # se queda esperando que conteste
            
            if respuesta == b"KO":
                print(f"\n[{ws_id} - ALERTA] ¡El Engine ha respondido KO (avería/fuga)!")
                if sock_central_global:
                    print(f"[{ws_id} - CLIENTE] Reportando 'ALERT#FUGA' a WM_Central...")
                    sock_central_global.sendall(b"ALERT#FUGA")
                break  # Paramos el pulso tras detectar el fallo
            elif respuesta != b"OK":
                if not respuesta:
                    print(f"[{ws_id} - SALUD] El Engine ha cerrado la conexión.")
                    break
                print(f"[{ws_id} - SALUD] Respuesta desconocida del Engine: {respuesta}")
        except Exception as e:
            print(f"[{ws_id} - SALUD] Error en el pulso con el Engine: {e}")
            break

#se ejecuta en el instante que el motor WM_WSE se conecta al puerto 5001
def handle_engine_connection(engine_socket, engine_address, ws_id):

    print(f"[{ws_id} - SERVIDOR] Engine conectado desde {engine_address}")
    # Lanzamos el vigilante de salud en un hilo independiente
    t_health = threading.Thread(target=health_pulse_worker, args=(engine_socket, ws_id), daemon=True)
    t_health.start()

def start_engine_server(listen_port, ws_id):
    """
    Abre el puerto local y se queda esperando a que el Engine llame a la puerta.
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", listen_port))
    server.listen(1)
    print(f"[{ws_id} - SERVIDOR] Esperando conexión local del Engine en el puerto {listen_port}...")
    
    while True:
        engine_sock, addr = server.accept()
        #crea un hilo en paralelo con la tarea handle_engine_connection, daemon=True significa que si cierras el monitor con ctrl +C se destruye
        #automáticamente el hilo
        t = threading.Thread(target=handle_engine_connection, args=(engine_sock, addr, ws_id), daemon=True)
        t.start()

# ==============================================================================
# ROL 2: CLIENTE (El Monitor llama por teléfono a WM_Central)
# ==============================================================================
#se encarga de llamar a la central, superar el protocolo de saludo, autenticar la estación con su identificador
# y ubicaciónm, y dejar guardado el canal de comunicación para futuras emergencias
def connect_to_central(central_ip, central_port, ws_id, ubicacion):
    #recube la dirección y puerto de la central, el identificador y s zona
    """
    Conecta como cliente a WM_Central, realiza el handshake inicial y se autentica.
    """
    global sock_central_global
    print(f"[{ws_id} - CLIENTE] Conectando con Central en {central_ip}:{central_port}...")
    try:
        #crea el socket local
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #marca el destno e inicia la conexión de tres vías
        client_sock.connect((central_ip, central_port))
        print(f"[{ws_id} - CLIENTE] Conexión física establecida. Enviando <ENQ>...")

        client_sock.sendall(ENQ)
        response = client_sock.recv(1024)
        if response != ACK: # se cierra el socket de inmediato
            print(f"[{ws_id} - CLIENTE] Central rechazó el apretón de manos inicial ({response}).")
            client_sock.close()
            return None

        print(f"[{ws_id} - CLIENTE] ¡<ACK> recibido de WM_Central! Enviando registro de autenticación...")
        #construme la trama convirtiendo el texto en bytes
        trama_registro = f"REG#{ws_id}#{ubicacion}".encode('utf-8')
        #envía las credenciales por socket
        client_sock.sendall(trama_registro)

        reg_response = client_sock.recv(1024)
        if reg_response == ACK:
            print(f"[{ws_id} - CLIENTE] ¡Autenticación OK! Estación registrada en Central como DISPONIBLE.")
            #asigna el socket abierto a sock_central_global
            sock_central_global = client_sock
            #devuelve el obketo del socket
            return client_sock
        else:
            print(f"[{ws_id} - CLIENTE] Error: Central rechazó el registro. Comprueba ID y Ubicación en la BD.")
            client_sock.close()
            return None

    except ConnectionRefusedError:
        print(f"[{ws_id} - CLIENTE] Error: No se pudo conectar a Central. Comprueba que WM_Central.py esté ejecutándose.")
        return None
    except Exception as e:
        print(f"[{ws_id} - CLIENTE] Error en la comunicación con Central: {e}")
        return None

# ==============================================================================
# PUNTO DE ARRANQUE DEL PROGRAMA
# ==============================================================================
if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("Uso: python3 WM_WS_M.py <ip_e> <puerto_e> <puerto_escucha_e> <ip_central> <puerto_central> <id_ws> [ubicacion]")
        print("Ejemplo: python3 WM_WS_M.py 127.0.0.1 6001 5001 127.0.0.1 8000 WS-01 'River Park'")
        sys.exit(1)

    ip_engine = sys.argv[1]
    puerto_engine = int(sys.argv[2])
    puerto_escucha_engine = int(sys.argv[3])
    ip_central = sys.argv[4]
    puerto_central = int(sys.argv[5])
    ws_id = sys.argv[6]
    ubicacion = sys.argv[7] if len(sys.argv) > 7 else "River Park"

    print("=" * 55)
    print(f"   INICIANDO MONITOR ({ws_id}) [CLIENTE & SERVIDOR]")
    print("=" * 55)

    # 1. Arrancar el servidor local para el Engine
    engine_server_thread = threading.Thread(
        target=start_engine_server, 
        args=(puerto_escucha_engine, ws_id),
        daemon=True
    )
    engine_server_thread.start()

    # 2. Conectarse y registrarse en Central
    sock_central = connect_to_central(ip_central, puerto_central, ws_id, ubicacion)

    # 3. Mantener el proceso vivo
    try:
        if sock_central:
            print(f"[{ws_id}] Canal con Central abierto y listo. Pulsa Ctrl+C para finalizar.\n")
        engine_server_thread.join()
    except KeyboardInterrupt:
        print(f"\n[{ws_id}] Monitor detenido manualmente.")
        if sock_central:
            sock_central.close()