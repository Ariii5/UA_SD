# ============================================================
#  WM_WS_M.py - Monitor de la estación de riego
#
#  El monitor hace dos cosas a la vez, cada una en su hilo:
#   1. CLIENTE de Central: se registra (ENQ/ACK + REG#id#ubicacion), le manda
#      un PING cada 2 s para que sepa que seguimos vivos y le avisa de las
#      averías (ALERT#FUGA) y de cuando se arreglan (ALERT#RESUELTA).
#   2. SERVIDOR para su engine: el engine se conecta a nosotros y cada
#      segundo le preguntamos HEALTH?. Si contesta KO o no contesta, es avería.
#
#  Uso: python3 WM_WS_M.py <ip_e> <puerto_e> <puerto_escucha_e> <ip_central> <puerto_central> <id_ws> [ubicacion]
# ============================================================
import sys
import socket
import threading
import time

# Bytes de control del protocolo con Central
ENQ = b'\x05'   # lo mandamos al conectar para ver si Central nos atiende
ACK = b'\x06'   # Central nos dice que sí
NACK = b'\x15'  # Central nos dice que no

#Socket con Central. Es global porque lo usan varios hilos
#(el del pulso de salud para avisar de averías y el del latido)
sock_central_global = None

TIMEOUT_PULSO = 3           # segundos que esperamos la respuesta del engine
estacion_averiada = False   # True desde que avisamos de una avería hasta que se arregla
LATIDO_CENTRAL = 2          # cada cuántos segundos mandamos PING a Central
lock_envio_central = threading.Lock()   # para que dos hilos no escriban a la vez en el socket



#Envíos a Central

# Manda una trama a Central. Todas las tramas acaban en "\n" para que Central
# sepa dónde termina cada una aunque le lleguen dos juntas
def avisar_central(ws_id, trama): #trama=mensaje (ALERT#FUGA)
    if not sock_central_global:
        print(f"[{ws_id} - CLIENTE] Sin conexión con Central: no se puede enviar '{trama}'.")
        return
    try:
        print(f"[{ws_id} - CLIENTE] Enviando '{trama}' a WM_Central...")
        with lock_envio_central:
            sock_central_global.sendall((trama + "\n").encode("utf-8"))
    except OSError as e:
        print(f"[{ws_id} - CLIENTE] No se pudo avisar a Central: {e}")


#Hilo que manda un PING a Central cada 2 s. Si Central deja de recibirlos
#durante 6 s, da la estación por DESCONECTADA (monitor colgado, red caída...)
def enviar_latidos():
    while True:
        time.sleep(LATIDO_CENTRAL)
        sock = sock_central_global #si se pone a none por otro proceso cuando entra en el try, salta un error q no captura osError,
                                    #por eso se mete en otra variable
        if sock:
            try:
                with lock_envio_central:
                    sock.sendall(b"PING\n")
            except OSError:
                #Si falla no hacemos nada aquí: la caída ya la detecta
                #mantener_conexion_central y se encarga de reconectar
                pass


#Avisa de la avería a Central solo la primera vez (para no mandar
#ALERT#FUGA cada segundo mientras dure)
def reportar_averia(ws_id, motivo):
    global estacion_averiada
    if estacion_averiada:
        return
    estacion_averiada = True
    print(f"\n[{ws_id} - ALERTA] {motivo}")
    avisar_central(ws_id, "ALERT#FUGA")


#Si veníamos de una avería y el engine vuelve a decir OK, avisamos a Central
def reportar_resuelta(ws_id):
    global estacion_averiada
    if not estacion_averiada:
        return
    estacion_averiada = False
    print(f"\n[{ws_id} - SALUD] El Engine vuelve a responder OK. Avería resuelta.")
    avisar_central(ws_id, "ALERT#RESUELTA")



#Servidor para el engine
# Pulso de salud: cada segundo le preguntamos HEALTH? al engine.
#   OK       -> todo bien (y si estaba averiado, avisamos de que se ha arreglado)
#   KO       -> avería, pero seguimos preguntando para saber cuándo se arregla
#   no responde en 3 s o se cierra la conexión -> avería y dejamos de preguntar
#   (cuando el engine vuelva a conectarse empezará un pulso nuevo)
def health_pulse_worker(engine_socket, ws_id):
    print(f"[{ws_id} - SALUD] Iniciando monitoreo periódico cada 1s con el Engine...")
    engine_socket.settimeout(TIMEOUT_PULSO)   #así el recv no se queda esperando para siempre, 3

    while True:
        time.sleep(1)
        try:
            engine_socket.sendall(b"HEALTH?")
            respuesta = engine_socket.recv(1024)
        except socket.timeout: #ya no pregunta
            reportar_averia(ws_id, f"El Engine no responde en {TIMEOUT_PULSO}s.")
            break
        except OSError as e:
            reportar_averia(ws_id, f"Conexión con el Engine perdida ({e}).") #ya no pregunta
            break

        if respuesta == b"OK": #sigue preguntando, pero no enviando ok
            reportar_resuelta(ws_id)
            continue
        if respuesta == b"KO": #sigue preguntando, pero no enviando ko
            reportar_averia(ws_id, "¡El Engine ha respondido KO (avería/fuga)!")
            continue
        if not respuesta:
            reportar_averia(ws_id, "El Engine ha cerrado la conexión.")
            break
        print(f"[{ws_id} - SALUD] Respuesta desconocida del Engine: {respuesta}") #si pasa cualquier otra cosa, alguien se conecta al mismo puerto

    engine_socket.close() #cuelga la llamada rota


#Se ejecuta cuando el engine se conecta: le decimos qué estación es
#(ID#WS-01) y arrancamos su pulso de salud en otro hilo
def handle_engine_connection(engine_socket, engine_address, ws_id): #este hilo termina una vez se conecta a health pulse worder que es el que se queda vvigilando
    print(f"[{ws_id} - SERVIDOR] Engine conectado desde {engine_address}")
    engine_socket.sendall(f"ID#{ws_id}".encode("utf-8"))
    t_health = threading.Thread(target=health_pulse_worker, args=(engine_socket, ws_id), daemon=True)
    t_health.start()


#Abre el puerto local y espera a que se conecte el engine. Si el engine se
#cae y vuelve, se acepta otra vez y empieza un pulso de salud nuevo
def start_engine_server(listen_port, ws_id):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #por red y tcp
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)   #poder reiniciar sin esperar a que se libere el puerto
    server.bind(("0.0.0.0", listen_port)) #le asigna el puerto, el 0.0 acepta llamadas desde cualquier camino
    server.listen(1) #se queda esperando como mucho 1 llamada mientras no se descuelga de la anterior
    print(f"[{ws_id} - SERVIDOR] Esperando conexión local del Engine en el puerto {listen_port}...")

    while True: #bucle porque el engine puede caerse y volver a un hilo nuevo
        engine_sock, addr = server.accept()
        # daemon=True: el hilo se cierra solo cuando cerramos el monitor con Ctrl+C
        t = threading.Thread(target=handle_engine_connection, args=(engine_sock, addr, ws_id), daemon=True)
        t.start()



#Cliente de Central

#Conecta con Central, hace el saludo ENQ/ACK y se registra con REG#id#ubicacion.
# Devuelve el socket si todo va bien, o None si algo falla
def connect_to_central(central_ip, central_port, ws_id, ubicacion):
    global sock_central_global
    print(f"[{ws_id} - CLIENTE] Conectando con Central en {central_ip}:{central_port}...")
    try:
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #por red y tcp
        client_sock.settimeout(5)   #si Central no contesta en 5 s, lo damos por fallido
        client_sock.connect((central_ip, central_port)) #tiene el timeout de arriba (5)
        print(f"[{ws_id} - CLIENTE] Conexión física establecida. Enviando <ENQ>...")

        #1. Saludo
        client_sock.sendall(ENQ)
        response = client_sock.recv(1024)
        if response != ACK:
            print(f"[{ws_id} - CLIENTE] Central rechazó el apretón de manos inicial ({response}).")
            client_sock.close()
            return None

        #2. Registro con nuestro id y ubicación
        print(f"[{ws_id} - CLIENTE] ¡<ACK> recibido de WM_Central! Enviando registro de autenticación...")
        trama_registro = f"REG#{ws_id}#{ubicacion}".encode('utf-8')
        client_sock.sendall(trama_registro)

        reg_response = client_sock.recv(1024)
        if reg_response == ACK:
            print(f"[{ws_id} - CLIENTE] ¡Autenticación OK! Estación registrada en Central como DISPONIBLE.")
            client_sock.settimeout(None)   #ya registrados: a partir de aquí esperamos sin límite
                                    #si no se pone el none, mantiene el timeout de 5 seg
            sock_central_global = client_sock
            return client_sock
        else:
            print(f"[{ws_id} - CLIENTE] Error: Central rechazó el registro. Comprueba ID y Ubicación en la BD.")
            client_sock.close()
            return None

    except ConnectionRefusedError: #la central no está encendida
        print(f"[{ws_id} - CLIENTE] Error: No se pudo conectar a Central. Comprueba que WM_Central.py esté ejecutándose.")
        return None
    except Exception as e: #cualquier otro error
        print(f"[{ws_id} - CLIENTE] Error en la comunicación con Central: {e}")
        return None


#Muestra por pantalla las órdenes que nos manda Central
def procesar_mensaje_central(ws_id, mensaje):
    if not mensaje:
        return
    if mensaje == "ORDEN#BLOQUEAR":
        print(f"\n[{ws_id} - CENTRAL]  ORDEN#BLOQUEAR recibida: la estación pasa a FUERA DE SERVICIO por orden de Central.")
    elif mensaje == "ORDEN#ACTIVAR":
        print(f"\n[{ws_id} - CENTRAL]  ORDEN#ACTIVAR recibida: la estación vuelve a estar ACTIVADA (disponible).")
    else:
        print(f"\n[{ws_id} - CENTRAL] Mensaje desconocido de Central: {mensaje}")

REINTENTO_CENTRAL = 3   #segundos entre intentos de reconexión con Central


#Bucle principal del cliente: conecta con Central y se queda escuchando.
#Si Central se cae, lo detectamos aquí y volvemos a intentarlo cada 3 s
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
            #Al registrarnos Central nos pone en DISPONIBLE, pero si la avería
            #sigue hay que volver a avisarle para que nos ponga en rojo
            avisar_central(ws_id, "ALERT#FUGA") #no se envia reportar_averia porque ya está en true estacion_Averiada

        #Escuchamos lo que nos manda Central. Mientras esté viva, recv espera;
        #si se cae, recv devuelve b"" o da error y salimos del bucle
        buffer = ""
        try:
            while True:
                datos = sock.recv(1024)
                if not datos:
                    break
                buffer += datos.decode("utf-8", "replace") #sustituye el buffer que habia con lo que habia + lo nuevo
                #Cada orden de Central acaba en "\n": procesamos las que estén completas
                while "\n" in buffer:
                    linea, buffer = buffer.split("\n", 1) #corta 1 vez en el \m
                    procesar_mensaje_central(ws_id, linea.strip())
        except OSError:
            pass

        print(f"\n[{ws_id} - CLIENTE] X Conexión con WM_Central perdida.")
        sock_central_global = None   #mientras no haya conexión, avisar_central no intenta enviar
        sock.close()



#Programa principal
if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("Uso: python3 WM_WS_M.py <ip_e> <puerto_e> <puerto_escucha_e> <ip_central> <puerto_central> <id_ws> [ubicacion]")
        print("Ejemplo: python3 WM_WS_M.py 127.0.0.1 6001 5001 127.0.0.1 8000 WS-01 'River Park'")
        sys.exit(1)

    #ip_engine y puerto_engine los pide el enunciado, pero no se usan:
    #es el engine quien se conecta a nosotros en puerto_escucha_engine
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

    #Hilo 1: servidor para el engine
    engine_server_thread = threading.Thread(
        target=start_engine_server,
        args=(puerto_escucha_engine, ws_id),
        daemon=True
    )
    engine_server_thread.start()

    #Hilo 2: latido hacia Central
    threading.Thread(target=enviar_latidos, daemon=True).start()

    #Hilo principal: conexión con Central (se reconecta sola si Central se cae)
    try:
        mantener_conexion_central(ip_central, puerto_central, ws_id, ubicacion)
    except KeyboardInterrupt:
        print(f"\n[{ws_id}] Monitor detenido manualmente.")
        if sock_central_global:
            sock_central_global.close()
