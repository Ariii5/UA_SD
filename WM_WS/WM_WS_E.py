# ============================================================
#  WM_WS_E.py - Engine de la estación de riego
#
#  Simula la electroválvula y el caudalímetro de la estación:
#   - Contesta OK o KO a los HEALTH? del monitor (por socket)
#   - Cuando Central le manda ABRIR (por Kafka) abre la válvula y cada
#     segundo publica el caudal y los litros que lleva
#   - Cierra cuando se acaba el tiempo, cuando Central manda CERRAR,
#     al pulsar una tecla o si se cierra el programa
#
#  Teclas: k = avería | o = reparar | a <seg> = riego manual | p = parar
#
#  Uso: py WM_WS_E.py <ip_broker:puerto> <ip_monitor> <puerto_monitor>
#  Ej.: py WM_WS_E.py localhost:9092 127.0.0.1 5001
# ============================================================
import sys
import socket
import threading
import time
import random
import json
import signal
import os
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import logging
logging.getLogger("kafka").addHandler(logging.NullHandler())   # para que kafka-python no llene la pantalla de avisos

simular_fuga = False        # True al pulsar k: contestamos KO al monitor
monitor_conectado = False   # sin monitor la estación está averiada y no riega

# Topics de Kafka
TOPIC_TELEMETRIA = "wm_telemetria"   # Engine -> Central y operarios
TOPIC_ORDENES = "wm_ordenes"         # Central -> Engine (ABRIR, CERRAR)
TOPIC_SOLICITUDES = "wm_solicitudes" # Engine -> Central (riego manual desde la estación)
TOPIC_RESPUESTAS = "wm_respuestas"   # Central -> quien pidió el riego (y el LATIDO)

MAX_ANTIGUEDAD_ORDEN = 4    # las órdenes con más de 4 s se ignoran
desfase_central = 0.0       # segundos que nuestro reloj va por delante del de Central (se calcula con su LATIDO)
producer = None             # productor de Kafka, se crea en el main
ws_id = None                # nuestro ID, nos lo dice el monitor al conectarnos
ultimo_latido = time.time() # última vez que recibimos el LATIDO de Central

# Carpeta donde guardamos lo que llevamos regado, por si el programa muere a mitad
DIR_DATOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datos")


# Publica un mensaje en Kafka añadiéndole nuestro ID y la hora
def publicar(topic, mensaje):
    mensaje["ws_id"] = ws_id
    mensaje["ts"] = time.time()
    try:
        producer.send(topic, key=ws_id, value=mensaje)
    except Exception as e:
        print(f"[ENGINE] Error publicando en Kafka: {e}")


# ------------------------------------------------------------
#  Fichero del riego en curso (para no perder lo regado)
# ------------------------------------------------------------
# Mientras riega, cada segundo guardamos en datos/riego_WS-XX.json lo que
# llevamos. Si el programa muere de golpe, al volver a arrancar lo leemos y
# mandamos a Central el final de ese riego

def fichero_estado():
    return os.path.join(DIR_DATOS, f"riego_{ws_id}.json")


# Primero se escribe en un .tmp y luego se renombra: así nunca queda un
# fichero a medias si el programa se corta justo mientras escribe
def guardar_estado(datos):
    os.makedirs(DIR_DATOS, exist_ok=True)
    tmp = fichero_estado() + ".tmp"
    with open(tmp, "w") as f:
        json.dump(datos, f)
    os.replace(tmp, fichero_estado())


def borrar_estado():
    try:
        os.remove(fichero_estado())
    except FileNotFoundError:
        pass


# Al arrancar: si hay un riego a medias guardado, mandamos su FIN a Central
def recuperar_riego():
    try:
        with open(fichero_estado()) as f:
            datos = json.load(f)
    except (FileNotFoundError, ValueError):
        return
    print(f"[ENGINE] Riego interrumpido encontrado ({datos['litros']} l). Enviando estado final a CENTRAL...")
    publicar(TOPIC_TELEMETRIA, {"tipo": "FIN", "motivo": "RECUPERADO_TRAS_CAIDA",
                                "duracion": datos["duracion"], "litros": datos["litros"],
                                "op_id": datos["op_id"], "req_id": datos["req_id"]})
    try:
        producer.flush(timeout=5)
        borrar_estado()
    except Exception:
        print("[ENGINE] No se pudo enviar el estado final; se reintentará en el próximo arranque.")


# ------------------------------------------------------------
#  Electroválvula y caudalímetro
# ------------------------------------------------------------
CAUDAL_NOMINAL = 12.0   # litros por minuto con la válvula abierta

# 'riego' lo tocan varios hilos (órdenes de Kafka, teclado, caudalímetro),
# así que siempre se usa con el lock
lock = threading.Lock()
riego = {
    "valvula_abierta": False,
    "inicio": 0.0,        # momento en que se abrió
    "duracion_max": 0,    # segundos que tiene que durar
    "caudal": 0.0,        # litros/minuto ahora mismo
    "volumen": 0.0,       # litros acumulados
    "op_id": None,        # operario que pidió el riego
    "req_id": None,       # id de la petición, para que Central sepa de qué riego hablamos
}


# Abre la válvula si se puede. Devuelve (True, "") o (False, motivo del rechazo)
def abrir_valvula(duracion, op_id=None, req_id=None):
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
    # Avisamos a Central de que hemos abierto (con esto autoriza al operario)
    publicar(TOPIC_TELEMETRIA, {"tipo": "INICIO", "duracion_max": duracion,
                                "op_id": op_id, "req_id": req_id})
    threading.Thread(target=bucle_caudalimetro, daemon=True).start()
    return True, ""


# Cierra la válvula y publica el FIN con el motivo, la duración y los litros
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
    borrar_estado()
    try:
        producer.flush(timeout=3)   # forzamos que el FIN salga ya, no se puede perder
    except Exception:
        # Si Kafka va lento el FIN se queda en la cola y se envía en cuanto
        # se pueda. No dejamos que el error pare el programa
        print("[ENGINE] Aviso: Kafka tarda en confirmar el FIN; se reintentará automáticamente.")


# Hilo que funciona mientras la válvula está abierta: cada segundo mide el
# caudal, suma los litros, lo guarda en el fichero y lo publica
def bucle_caudalimetro():
    while True:
        time.sleep(1)
        with lock:
            if not riego["valvula_abierta"]:
                return
            # El caudal varía un poco alrededor de 12 l/min para que parezca real
            caudal = round(CAUDAL_NOMINAL + random.uniform(-1.5, 1.5), 2)
            riego["caudal"] = caudal
            riego["volumen"] += caudal / 60      # l/min -> litros en 1 segundo
            volumen = riego["volumen"]
            transcurrido = time.time() - riego["inicio"]
            limite = riego["duracion_max"]
            op_id = riego["op_id"]
            req_id = riego["req_id"]
            guardar_estado({"op_id": op_id, "req_id": req_id, "litros": round(volumen, 2),
                            "duracion": round(transcurrido, 1)})
        publicar(TOPIC_TELEMETRIA, {"tipo": "TELEMETRIA", "caudal": caudal,
                                    "volumen": round(volumen, 2),
                                    "transcurrido": round(transcurrido, 1),
                                    "op_id": op_id, "req_id": req_id})
        print(f"[ENGINE] Regando | caudal {caudal:5.2f} l/min | "
              f"acumulado {volumen:7.2f} l | {transcurrido:4.1f}/{limite}s")
        if transcurrido >= limite:
            cerrar_valvula("TIEMPO_LIMITE")
            return


# ------------------------------------------------------------
#  Teclado
# ------------------------------------------------------------
# Va en su propio hilo para que esperar una tecla no pare lo demás
def keyboard_listener():
    global simular_fuga
    while True:
        try:
            cmd = input().strip().lower()
        except EOFError:
            return
        if cmd == 'k':
            # Avería: cortamos el riego y en el próximo HEALTH? contestamos KO
            simular_fuga = True
            print("\n[ENGINE] ¡FALLO FORZADO! En el próximo pulso de salud se enviará 'KO'...\n")
            cerrar_valvula("FUGA")
        elif cmd.startswith('a'):
            # Riego manual: no abrimos directamente, se lo pedimos a Central
            # como si fuéramos un operario, para que lo valide y lo registre
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
        elif cmd == 'o':
            simular_fuga = False
            print("\n[ENGINE] Avería reparada. En el próximo pulso de salud se enviará 'OK'.\n")


# ------------------------------------------------------------
#  Órdenes de Central por Kafka
# ------------------------------------------------------------

# Hilo que lee las órdenes (ABRIR, CERRAR) y las respuestas de Central.
# También recoge el LATIDO de Central para saber si sigue viva
def escuchar_ordenes(broker):
    global ultimo_latido, desfase_central
    consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        group_id=f"wm_engine_{ws_id}",   # un grupo propio para cada estación
        auto_offset_reset="latest",      # solo órdenes nuevas
        metadata_max_age_ms=3000,        # refresca la información de Kafka cada 3 s
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    consumer.subscribe([TOPIC_ORDENES, TOPIC_RESPUESTAS])
    # Al arrancar saltamos al final de los topics: no queremos abrir la
    # válvula por un ABRIR que se quedó en Kafka mientras estábamos apagados
    limite = time.time() + 30
    while not consumer.assignment() and time.time() < limite:
        consumer.poll(timeout_ms=200)
    if consumer.assignment():
        consumer.seek_to_end(*consumer.assignment())
        for tp in consumer.assignment():
            consumer.position(tp)
    while True:
        lotes = consumer.poll(timeout_ms=500)
        for registros in lotes.values():
            for msg in registros:
                orden = msg.value
                if msg.topic == TOPIC_RESPUESTAS:
                    if orden.get("tipo") == "LATIDO":
                        ultimo_latido = time.time()
                        # El LATIDO lleva la hora de Central: comparándola con la
                        # nuestra sabemos cuánto se desvía nuestro reloj del suyo
                        desfase_central = time.time() - orden.get("ts", time.time())
                    elif orden.get("op_id") == f"MANUAL-{ws_id}":
                        # Respuesta a un riego manual pedido desde esta estación
                        print(f"[ENGINE] CENTRAL ({orden.get('tipo')}): {orden.get('mensaje')}")
                    continue
                if orden.get("ws_id") != ws_id:
                    continue        # la orden es para otra estación
                # La antigüedad se calcula con la hora de Central (nuestro reloj
                # corregido con el desfase). Así, aunque el PC tenga la hora mal,
                # no descartamos órdenes buenas
                if (time.time() - desfase_central) - orden.get("ts", 0) > MAX_ANTIGUEDAD_ORDEN:
                    continue        # orden demasiado vieja
                procesar_orden(orden)


# Hilo que avisa si Central deja de mandar su LATIDO durante más de 6 s
def vigilar_central():
    caida = False
    while True:
        time.sleep(1)
        if time.time() - ultimo_latido > 6:
            if not caida:
                print("\n[ENGINE] ✖ Imposible conectar con WM_Central / gestor de colas.")
                caida = True
        elif caida:
            print("\n[ENGINE] ✔ WM_Central vuelve a estar disponible.")
            caida = False


def procesar_orden(orden):
    tipo = orden.get("tipo")
    if tipo == "ABRIR":
        print(f"\n[ENGINE] Orden de CENTRAL: ABRIR (operario {orden.get('op_id')})")
        ok, motivo = abrir_valvula(int(orden.get("duracion", 10)),
                                   orden.get("op_id"), orden.get("req_id"))
        if not ok:
            # No hemos podido abrir: se lo decimos a Central para que deniegue
            print(f"[ENGINE] Rechazada: {motivo}")
            publicar(TOPIC_TELEMETRIA, {"tipo": "RECHAZO", "motivo": motivo,
                                        "op_id": orden.get("op_id"),
                                        "req_id": orden.get("req_id")})
    elif tipo == "CERRAR":
        print(f"\n[ENGINE] Orden de CENTRAL: CERRAR")
        cerrar_valvula(orden.get("motivo", "ORDEN_CENTRAL"))


# ------------------------------------------------------------
#  Cerrar la válvula si se cierra la ventana o se para el proceso
# ------------------------------------------------------------
def instalar_cierre_ventana():
    if sys.platform == "win32":
        # En Windows, cerrar la ventana (la X) no le llega a Python como una
        # señal normal: hay que registrarse en la consola con SetConsoleCtrlHandler
        import ctypes
        CTRL_CLOSE_EVENT = 2       # código que manda Windows al pulsar la X
        TipoManejador = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.c_uint)

        def manejador(evento):
            if evento == CTRL_CLOSE_EVENT:
                cerrar_valvula("CIERRE_VENTANA")
                return 1           # lo hemos gestionado nosotros
            return 0               # Ctrl+C y lo demás lo gestiona Python como siempre

        # Se guarda en una variable global: si Python la borrara, Windows
        # llamaría a una función que ya no existe y el programa se rompería
        global _manejador_windows
        _manejador_windows = TipoManejador(manejador)
        ctypes.windll.kernel32.SetConsoleCtrlHandler(_manejador_windows, True)
    else:
        # Linux / Mac / Docker: cerrar la ventana manda SIGHUP y docker stop manda SIGTERM
        def manejador_senal(signum, frame):
            motivo = "CIERRE_VENTANA" if signum == signal.SIGHUP else "PROCESO_DETENIDO"
            cerrar_valvula(motivo)
            sys.exit(0)
        signal.signal(signal.SIGHUP, manejador_senal)
        signal.signal(signal.SIGTERM, manejador_senal)


# Si perdemos el monitor, lo volvemos a intentar cada 3 s hasta que vuelva
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


# ------------------------------------------------------------
#  Programa principal
# ------------------------------------------------------------
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
        # 1. Nos conectamos al monitor y él nos dice qué estación somos
        sock.connect((ip_monitor, puerto_monitor))
        print(f"[ENGINE] Conectado al Monitor en {ip_monitor}:{puerto_monitor}")
        primera = sock.recv(1024).decode("utf-8").strip()
        if not primera.startswith("ID#"):
            print(f"[ENGINE] Error: esperaba ID#<id> y llegó {primera!r}")
            sys.exit(1)
        ws_id = primera.split("#", 1)[1]
        print(f"[ENGINE] Estación asignada: {ws_id}")
        monitor_conectado = True

        # 2. Conexión con Kafka
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
                max_block_ms=3000,
            )
        except NoBrokersAvailable:
            print(f"[ENGINE] Error: no hay ningún Kafka escuchando en {broker}")
            sys.exit(1)
        print(f"[ENGINE] Conectado a Kafka en {broker}")
        recuperar_riego()   # por si nos caímos a mitad de un riego

        # 3. Hilos: órdenes de Central y vigilancia del latido
        threading.Thread(target=escuchar_ordenes, args=(broker,), daemon=True).start()
        ultimo_latido = time.time()
        threading.Thread(target=vigilar_central, daemon=True).start()

        # A partir de aquí, cerrar la ventana cierra la válvula de forma ordenada
        instalar_cierre_ventana()

        print("[ENGINE] Respondiendo 'OK' a los pulsos de salud.")
        print(f"[ENGINE] PID: {os.getpid()}")
        print("[ENGINE] Teclas: k = avería | o = reparar avería | a <seg> = pedir riego manual a CENTRAL | p = parar riego\n")

        threading.Thread(target=keyboard_listener, daemon=True).start()

        # 4. Bucle principal: contestar a los HEALTH? del monitor
        sock.settimeout(1.0)   # recv espera como mucho 1 s, así Ctrl+C funciona también en Windows
        while True:
            try:
                data = sock.recv(1024)
            except socket.timeout:
                continue
            except OSError:
                data = b""
            if not data:
                # Se ha caído el monitor: la estación queda averiada, cortamos el
                # riego y esperamos a que vuelva
                monitor_conectado = False
                print("\n[ENGINE] ESTACIÓN AVERIADA: se ha perdido el Monitor.")
                cerrar_valvula("MONITOR_CAIDO")
                sock.close()
                sock = reconectar_monitor(ip_monitor, puerto_monitor)
                monitor_conectado = True
                print("[ENGINE] Monitor recuperado. Estación operativa.")
                continue

            if data == b"HEALTH?":
                try:
                    if simular_fuga:
                        sock.sendall(b"KO")
                        print("[ENGINE] Enviado: KO -> Avería reportada.")
                    else:
                        sock.sendall(b"OK")
                except OSError:
                    # El monitor ya había cerrado el socket (por ejemplo, porque
                    # tardamos en contestar). En la siguiente vuelta recv()
                    # devolverá b"" y nos reconectaremos
                    continue
    except ConnectionRefusedError:
        print(f"[ENGINE] Error: El Monitor no está escuchando en {ip_monitor}:{puerto_monitor}")
    except KeyboardInterrupt:
        print("\n[ENGINE] Detenido con Ctrl+C.")
    except Exception as e:
        print(f"[ENGINE] Desconexión: {e}")
    finally:
        # Pase lo que pase, nunca dejamos el agua abierta al salir
        if producer:
            cerrar_valvula("PROCESO_DETENIDO")
            try:
                producer.close(timeout=3)   # envía lo pendiente y suelta la conexión con Kafka
            except Exception:
                pass
        sock.close()
