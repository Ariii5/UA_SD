# WM_WS_E.py - Engine de la estación de riego
# Misión:
# - Responder OK/KO a los chequeos periódicos de salud (HEALTH?) del Monitor
# - Simular una avería (tecla k)
# - Simular la electroválvula y el caudalímetro
# - Publicar en Kafka el inicio, la telemetría (cada segundo) y el fin del riego
# - Cerrar el riego al cerrar la ventana / detener el proceso
# - Obedecer las órdenes ABRIR / CERRAR que le manda CENTRAL por Kafka
# - Riego manual desde la propia estación (tecla a), que también pasa por CENTRAL
#
# Uso: py WM_WS_E.py <ip_broker:puerto> <ip_monitor> <puerto_monitor>
# Ej.: py WM_WS_E.py localhost:9092 127.0.0.1 5001
import sys
import socket
import threading
import time
import random
import json
import signal
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

simular_fuga = False
monitor_conectado = False

#Kafka
TOPIC_TELEMETRIA = "wm_telemetria"   # Engine -> Central y operarios
TOPIC_ORDENES = "wm_ordenes"         # Central -> Engine
TOPIC_SOLICITUDES = "wm_solicitudes" # Engine -> Central (riego manual desde la estación)
TOPIC_RESPUESTAS = "wm_respuestas"   # Central -> quien pidió el riego
MAX_ANTIGUEDAD_ORDEN = 30            # s: órdenes más viejas se ignoran
producer = None   # se crea en el main, cuando ya sabemos el ws_id
ws_id = None      # nos lo envía el Monitor al conectarnos


def publicar(topic, mensaje):
    """Envía un diccionario a Kafka (como JSON) añadiendo el id de la estación y la hora."""
    mensaje["ws_id"] = ws_id
    mensaje["ts"] = time.time()
    try:
        producer.send(topic, key=ws_id, value=mensaje)
    except Exception as e:
        print(f"[ENGINE] Error publicando en Kafka: {e}")


#electroválvula y caudalímetro
CAUDAL_NOMINAL = 12.0   # litros/minuto con la válvula abierta

lock = threading.Lock()  # protege 'riego', que lo tocan varios hilos
riego = {
    "valvula_abierta": False,
    "inicio": 0.0,        # momento en que se abrió (segundos)
    "duracion_max": 0,    # tiempo límite del riego (segundos)
    "caudal": 0.0,        # litros/minuto ahora mismo
    "volumen": 0.0,       # litros acumulados desde que se abrió
    "op_id": None,        # operario que pidió el riego
    "req_id": None,       # id de la petición, para que Central sepa a qué riego se refiere
}


def abrir_valvula(duracion, op_id=None, req_id=None):
    """Abre la válvula si se puede. Devuelve (True, "") o (False, motivo del rechazo)."""
    with lock:
        if not monitor_conectado:
            return False, "La estación está averiada (sin monitor)"
        if simular_fuga:
            return False, "La estación tiene una fuga/avería"
        if riego["valvula_abierta"]:
            return False, f"La estación ya está regando (operario {riego['op_id']})"
        riego["valvula_abierta"] = True
        riego["inicio"] = time.time()
        riego["duracion_max"] = duracion
        riego["caudal"] = 0.0
        riego["volumen"] = 0.0
        riego["op_id"] = op_id
        riego["req_id"] = req_id
    print(f"\n[ENGINE] >>> ELECTROVÁLVULA ABIERTA (operario {op_id}, límite {duracion}s)")
    publicar(TOPIC_TELEMETRIA, {"tipo": "INICIO", "duracion_max": duracion,
                                "op_id": op_id, "req_id": req_id})
    threading.Thread(target=bucle_caudalimetro, daemon=True).start()
    return True, ""


def cerrar_valvula(motivo):
    with lock:
        if not riego["valvula_abierta"]:
            return
        riego["valvula_abierta"] = False
        riego["caudal"] = 0.0
        duracion = round(time.time() - riego["inicio"], 1)
        litros = round(riego["volumen"], 2)
        op_id = riego["op_id"]
        req_id = riego["req_id"]
    print(f"\n[ENGINE] <<< ELECTROVÁLVULA CERRADA. Motivo: {motivo} | "
          f"Duración: {duracion}s | Litros: {litros}")
    publicar(TOPIC_TELEMETRIA, {"tipo": "FIN", "motivo": motivo,
                                "duracion": duracion, "litros": litros,
                                "op_id": op_id, "req_id": req_id})
    try:
        producer.flush(timeout=3)   # obliga a enviar ya lo pendiente: el FIN no se puede perder
    except Exception:
        # Kafka va lento: el FIN sigue en la cola del productor y se enviará en cuanto se pueda.
        # NO dejamos que el error rompa el hilo
        print("[ENGINE] Aviso: Kafka tarda en confirmar el FIN; se reintentará automáticamente.")


def bucle_caudalimetro():
    """Hilo que vive mientras la válvula está abierta: cada segundo mide y publica."""
    while True:
        time.sleep(1)
        with lock:
            if not riego["valvula_abierta"]:
                return
            caudal = round(CAUDAL_NOMINAL + random.uniform(-1.5, 1.5), 2)
            riego["caudal"] = caudal
            riego["volumen"] += caudal / 60      # l/min -> litros en 1 segundo
            volumen = riego["volumen"]
            transcurrido = time.time() - riego["inicio"]
            limite = riego["duracion_max"]
            op_id = riego["op_id"]
            req_id = riego["req_id"]
        publicar(TOPIC_TELEMETRIA, {"tipo": "TELEMETRIA", "caudal": caudal,
                                    "volumen": round(volumen, 2),
                                    "transcurrido": round(transcurrido, 1),
                                    "op_id": op_id, "req_id": req_id})
        print(f"[ENGINE] Regando | caudal {caudal:5.2f} l/min | "
              f"acumulado {volumen:7.2f} l | {transcurrido:4.1f}/{limite}s")
        if transcurrido >= limite:
            cerrar_valvula("TIEMPO_LIMITE")
            return


#Teclado
def keyboard_listener():
    """Escucha la pulsación de teclas en la terminal."""
    global simular_fuga
    while True:
        try:
            cmd = input().strip().lower()
        except EOFError:
            return
        if cmd == 'k':
            simular_fuga = True
            print("\n[ENGINE] ¡FALLO FORZADO! En el próximo pulso de salud se enviará 'KO'...\n")
            cerrar_valvula("FUGA")
        elif cmd.startswith('a'):
            # Riego manual desde la estación: NO abrimos directamente, se lo pedimos
            # a CENTRAL como si fuéramos un operario, para que valide y lo registre.
            if not monitor_conectado:
                print("[ENGINE] ESTACIÓN AVERIADA: sin monitor. No se admiten riegos.")
                continue
            partes = cmd.split()
            duracion = int(partes[1]) if len(partes) > 1 and partes[1].isdigit() else 10
            publicar(TOPIC_SOLICITUDES, {"tipo": "SOLICITUD",
                                         "req_id": f"MAN-{ws_id}-{int(time.time())}",
                                         "op_id": f"MANUAL-{ws_id}",
                                         "duracion": duracion})
            print(f"[ENGINE] Petición de riego manual ({duracion}s) enviada a CENTRAL...")
        elif cmd == 'p':
            cerrar_valvula("PARADA_MANUAL")


#ordenes por kafka
def escuchar_ordenes(broker):
    """Hilo consumidor: obedece las órdenes para esta estación y muestra las
    respuestas de CENTRAL a los riegos manuales pedidos desde aquí."""
    consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        group_id=f"wm_engine_{ws_id}",   # un grupo propio por estación
        auto_offset_reset="latest",      # solo órdenes nuevas
        metadata_max_age_ms=3000,        # detectar pronto el topic si se crea después
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    consumer.subscribe([TOPIC_ORDENES, TOPIC_RESPUESTAS])
    while True:
        lotes = consumer.poll(timeout_ms=500)
        for registros in lotes.values():
            for msg in registros:
                orden = msg.value
                if msg.topic == TOPIC_RESPUESTAS:
                    # Solo nos interesan las respuestas a NUESTRAS peticiones manuales
                    if orden.get("op_id") == f"MANUAL-{ws_id}":
                        print(f"[ENGINE] CENTRAL ({orden.get('tipo')}): {orden.get('mensaje')}")
                    continue
                if orden.get("ws_id") != ws_id:
                    continue        # la orden es para otra estación
                if time.time() - orden.get("ts", 0) > MAX_ANTIGUEDAD_ORDEN:
                    continue        # orden caducada
                procesar_orden(orden)


def procesar_orden(orden):
    tipo = orden.get("tipo")
    if tipo == "ABRIR":
        print(f"\n[ENGINE] Orden de CENTRAL: ABRIR (operario {orden.get('op_id')})")
        ok, motivo = abrir_valvula(int(orden.get("duracion", 10)),
                                   orden.get("op_id"), orden.get("req_id"))
        if not ok:
            print(f"[ENGINE] Rechazada: {motivo}")
            publicar(TOPIC_TELEMETRIA, {"tipo": "RECHAZO", "motivo": motivo,
                                        "op_id": orden.get("op_id"),
                                        "req_id": orden.get("req_id")})
    elif tipo == "CERRAR":
        print(f"\n[ENGINE] Orden de CENTRAL: CERRAR")
        cerrar_valvula(orden.get("motivo", "ORDEN_CENTRAL"))


#cierra la ventana
def instalar_cierre_ventana():
    """Hace que, si se cierra la ventana (la X) o se mata el proceso, se cierre
    la electroválvula y se publique el FIN antes de morir."""
    if sys.platform == "win32":
        # En Windows, cerrar la ventana NO llega a Python como señal: hay que
        # registrarse en la consola con la función SetConsoleCtrlHandler de Windows.
        import ctypes
        CTRL_CLOSE_EVENT = 2       # código que manda Windows al pulsar la X
        TipoManejador = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.c_uint)

        def manejador(evento):
            if evento == CTRL_CLOSE_EVENT:
                cerrar_valvula("CIERRE_VENTANA")
                return 1           # "lo he gestionado yo"
            return 0               # Ctrl+C y demás: que lo gestione Python como siempre

        # Guardamos la referencia en una global: si Python la borrara, Windows
        # llamaría a una función que ya no existe y el programa reventaría.
        global _manejador_windows
        _manejador_windows = TipoManejador(manejador)
        ctypes.windll.kernel32.SetConsoleCtrlHandler(_manejador_windows, True)
    else:
        # Linux / macOS / Docker: cerrar la ventana manda SIGHUP; docker stop manda SIGTERM
        def manejador_senal(signum, frame):
            motivo = "CIERRE_VENTANA" if signum == signal.SIGHUP else "PROCESO_DETENIDO"
            cerrar_valvula(motivo)
            sys.exit(0)
        signal.signal(signal.SIGHUP, manejador_senal)
        signal.signal(signal.SIGTERM, manejador_senal)


def reconectar_monitor(ip, puerto):
    while True:
        time.sleep(3)
        try:
            s = socket.create_connection((ip, puerto), timeout=3)
            s.settimeout(5)
            if s.recv(1024).decode("utf-8").startswith("ID#"):
                s.settimeout(1.0)
                return s
            s.close()
        except OSError:
            pass
        print(f"[ENGINE] Monitor no disponible en {ip}:{puerto}. Reintentando en 3 s...")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Uso: py WM_WS_E.py <ip_broker:puerto> <ip_monitor> <puerto_monitor>")
        print("Ejemplo: py WM_WS_E.py localhost:9092 127.0.0.1 5001")
        sys.exit(1)

    broker = sys.argv[1]
    ip_monitor = sys.argv[2]
    puerto_monitor = int(sys.argv[3])

    print("=" * 50)
    print("      SIMULADOR DEL MOTOR (WM_WS_E)")
    print("=" * 50)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        #Conexión con el Monitor y recepción del ID
        sock.connect((ip_monitor, puerto_monitor))
        print(f"[ENGINE] Conectado al Monitor en {ip_monitor}:{puerto_monitor}")
        primera = sock.recv(1024).decode("utf-8").strip()
        if not primera.startswith("ID#"):
            print(f"[ENGINE] Error: esperaba ID#<id> y llegó {primera!r}")
            sys.exit(1)
        ws_id = primera.split("#", 1)[1]
        print(f"[ENGINE] Estación asignada: {ws_id}")
        monitor_conectado = True

        #Conexión con Kafka
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print(f"[ENGINE] Error: no hay ningún Kafka escuchando en {broker}")
            sys.exit(1)
        print(f"[ENGINE] Conectado a Kafka en {broker}")

        #Hilo que escucha las órdenes de Central
        threading.Thread(target=escuchar_ordenes, args=(broker,), daemon=True).start()

        #A partir de aquí, cerrar la ventana cierra la válvula de forma ordenada
        instalar_cierre_ventana()

        print("[ENGINE] Respondiendo 'OK' a los pulsos de salud.")
        print("[ENGINE] Teclas: k = avería | a <seg> = pedir riego manual a CENTRAL | p = parar riego\n")

        #Hilo para leer el teclado en paralelo
        threading.Thread(target=keyboard_listener, daemon=True).start()

        #Bucle principal: pulsos de salud del Monitor
        sock.settimeout(1.0)   # recv espera como mucho 1 s: así Ctrl+C funciona en Windows
        while True:
            try:
                data = sock.recv(1024)
            except socket.timeout:
                continue
            except OSError:
                data = b""
            if not data:
                monitor_conectado = False
                print("\n[ENGINE] ESTACIÓN AVERIADA: se ha perdido el Monitor.")
                cerrar_valvula("MONITOR_CAIDO")
                sock.close()
                sock = reconectar_monitor(ip_monitor, puerto_monitor)
                monitor_conectado = True
                print("[ENGINE] Monitor recuperado. Estación operativa.")
                continue

            if data == b"HEALTH?":
                if simular_fuga:
                    sock.sendall(b"KO")
                    print("[ENGINE] Enviado: KO -> Avería reportada.")
                else:
                    sock.sendall(b"OK")
    except ConnectionRefusedError:
        print(f"[ENGINE] Error: El Monitor no está escuchando en {ip_monitor}:{puerto_monitor}")
    except KeyboardInterrupt:
        print("\n[ENGINE] Detenido con Ctrl+C.")
    except Exception as e:
        print(f"[ENGINE] Desconexión: {e}")
    finally:
        if producer:
            cerrar_valvula("PROCESO_DETENIDO")   #nunca dejar el agua corriendo al salir
            try:
                producer.close(timeout=3)        #envía lo pendiente y suelta la conexión con Kafka
            except Exception:
                pass
        sock.close()