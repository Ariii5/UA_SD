import sys
import socket
import threading
import time
#su mision es revisar WM_WS_E y avisar a la central si detecta un fallo
#bytes de control
ENQ = b'\x05'   #Lo enviamos nosotros a Central para ver si alguien escucha
ACK = b'\x06'   #Esperamos que Central nos conteste
NACK = b'\x15'  #Error

# Variables globales para compartir los sockets entre hilos
sock_central_global = None

TIMEOUT_PULSO = 3   # segundos máximos esperando la respuesta del Engine
estacion_averiada = False   # True desde que avisamos de una avería hasta que se resuelve
LATIDO_CENTRAL = 2          # s entre PING y PING a Central ("sigo vivo")
lock_envio_central = threading.Lock()   # el pulso de salud y el latido envían desde hilos distintos


def avisar_central(ws_id, trama):
    """Envía una trama de estado a Central por el socket ya autenticado."""
    if not sock_central_global:
        print(f"[{ws_id} - CLIENTE] Sin conexión con Central: no se puede enviar '{trama}'.")
        return
    try:
        print(f"[{ws_id} - CLIENTE] Enviando '{trama}' a WM_Central...")
        with lock_envio_central:
            sock_central_global.sendall((trama + "\n").encode("utf-8"))   # "\n" marca el fin de la trama
    except OSError as e:
        print(f"[{ws_id} - CLIENTE] No se pudo avisar a Central: {e}")


def enviar_latidos():
    """Hilo: manda PING a Central cada LATIDO_CENTRAL s mientras haya conexión.
    Así Central puede detectar un Monitor colgado o una red cortada sin cierre
    (la guía: 'Desconectado = Central no recibe ningún mensaje de vida del Monitor')."""
    while True:
        time.sleep(LATIDO_CENTRAL)
        sock = sock_central_global
        if sock:
            try:
                with lock_envio_central:
                    sock.sendall(b"PING\n")
            except OSError:
                pass   # la caída la detecta mantener_conexion_central con su recv()


def reportar_averia(ws_id, motivo):
    """Primera vez que se detecta el fallo: avisa a Central (ROJO). Si ya estaba avisado, no repite."""
    global estacion_averiada
    if estacion_averiada:
        return
    estacion_averiada = True
    print(f"\n[{ws_id} - ALERTA] {motivo}")
    avisar_central(ws_id, "ALERT#FUGA")


def reportar_resuelta(ws_id):
    """El Engine vuelve a responder OK tras una avería: avisa a Central (VERDE)."""
    global estacion_averiada
    if not estacion_averiada:
        return
    estacion_averiada = False
    print(f"\n[{ws_id} - SALUD] El Engine vuelve a responder OK. Avería resuelta.")
    avisar_central(ws_id, "ALERT#RESUELTA")


def health_pulse_worker(engine_socket, ws_id):
    print(f"[{ws_id} - SALUD] Iniciando monitoreo periódico cada 1s con el Engine...")
    engine_socket.settimeout(TIMEOUT_PULSO)   # el recv ya no puede bloquearse para siempre

    while True:
        time.sleep(1)
        try:
            engine_socket.sendall(b"HEALTH?")
            respuesta = engine_socket.recv(1024)
        except socket.timeout:
            reportar_averia(ws_id, f"El Engine no responde en {TIMEOUT_PULSO}s.")
            break
        except OSError as e:
            reportar_averia(ws_id, f"Conexión con el Engine perdida ({e}).")
            break

        if respuesta == b"OK":
            reportar_resuelta(ws_id)      # solo hace algo si veníamos de una avería
            continue
        if respuesta == b"KO":
            reportar_averia(ws_id, "¡El Engine ha respondido KO (avería/fuga)!")
            continue                      # seguimos vigilando para detectar la reparación
        if not respuesta:
            reportar_averia(ws_id, "El Engine ha cerrado la conexión.")
            break
        print(f"[{ws_id} - SALUD] Respuesta desconocida del Engine: {respuesta}")

    engine_socket.close()

#se ejecuta en el instante que el motor WM_WSE se conecta al puerto 5001
def handle_engine_connection(engine_socket, engine_address, ws_id):

    print(f"[{ws_id} - SERVIDOR] Engine conectado desde {engine_address}")
    engine_socket.sendall(f"ID#{ws_id}".encode("utf-8")) #antes de iniciar el pulso de salud, le enviamos nuestro identificador para saber quien es
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

#CLIENTE (El Monitor llama por teléfono a WM_Central)
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
        client_sock.settimeout(5)
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
            client_sock.settimeout(None) 
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

def procesar_mensaje_central(ws_id, mensaje):
    if not mensaje:
        return
    if mensaje == "ORDEN#BLOQUEAR":
        print(f"\n[{ws_id} - CENTRAL] ⛔ ORDEN#BLOQUEAR recibida: la estación pasa a FUERA DE SERVICIO por orden de Central.")
    elif mensaje == "ORDEN#ACTIVAR":
        print(f"\n[{ws_id} - CENTRAL] ✅ ORDEN#ACTIVAR recibida: la estación vuelve a estar ACTIVADA (disponible).")
    else:
        print(f"\n[{ws_id} - CENTRAL] Mensaje desconocido de Central: {mensaje}")

REINTENTO_CENTRAL = 3   # segundos entre intentos de reconexión con Central


def mantener_conexion_central(central_ip, central_port, ws_id, ubicacion):
    global sock_central_global
    while True:
        sock = connect_to_central(central_ip, central_port, ws_id, ubicacion)
        if sock is None:
            print(f"[{ws_id} - CLIENTE] Reintentando conexión con Central en {REINTENTO_CENTRAL}s...")
            time.sleep(REINTENTO_CENTRAL)
            continue

        print(f"[{ws_id}] Canal con Central abierto y listo. Pulsa Ctrl+C para finalizar.\n")
        if estacion_averiada:
            # Central nos acaba de registrar como DISPONIBLE, pero la avería sigue: se lo recordamos
            avisar_central(ws_id, "ALERT#FUGA")

        # Escuchamos a Central. Mientras siga viva, recv se queda esperando;
        # si se cae, recv devuelve b"" (cierre) o lanza un error (conexión reseteada).
        buffer = ""
        try:
            while True:
                datos = sock.recv(1024)
                if not datos:
                    break
                buffer += datos.decode("utf-8", "replace")
                # Central termina cada orden con "\n": procesamos las líneas completas
                while "\n" in buffer:
                    linea, buffer = buffer.split("\n", 1)
                    procesar_mensaje_central(ws_id, linea.strip())
        except OSError:
            pass

        print(f"\n[{ws_id} - CLIENTE] ✖ Conexión con WM_Central perdida.")
        sock_central_global = None   # mientras tanto, avisar_central no intentará enviar
        sock.close()

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

    #Arrancar el servidor local para el Engine
    engine_server_thread = threading.Thread(
        target=start_engine_server, 
        args=(puerto_escucha_engine, ws_id),
        daemon=True
    )
    engine_server_thread.start()

    #Latido periódico hacia Central
    threading.Thread(target=enviar_latidos, daemon=True).start()

        #Conectarse a Central y mantener la conexión (reconecta solo si Central se cae)
    try:
        mantener_conexion_central(ip_central, puerto_central, ws_id, ubicacion)
    except KeyboardInterrupt:
        print(f"\n[{ws_id}] Monitor detenido manualmente.")
        if sock_central_global:
            sock_central_global.close()