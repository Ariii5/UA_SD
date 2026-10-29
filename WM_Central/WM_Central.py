# ============================================================
#  WM_Central.py - Central del sistema de riego
#
#  Qué hace:
#   - Servidor de sockets para los monitores de las estaciones: los registra
#     (comprobándolos en SQLite), recibe sus PING y sus avisos de avería y
#     les manda las órdenes de bloquear y activar
#   - Kafka: recibe las peticiones de riego de los operarios, comprueba que
#     la estación está disponible, le manda ABRIR al engine, sigue la
#     telemetría y al final le manda el resumen al operario
#   - Panel en la terminal (en local) y panel web (para Railway)
#   - Comandos de administración: regar, bloquear, activar y dar de alta
#
#  Uso: python3 WM_Central.py <puerto_sockets> <ip_kafka:puerto> [puerto_web]
# ============================================================
import sys
import json
import io
import time
import uuid
import socket
import threading
import re
import os
import hmac
import base64
import collections
import http.server
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
from prompt_toolkit.application import Application
from prompt_toolkit.formatted_text import ANSI
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.layout.dimension import Dimension
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.widgets import TextArea
from db_manager import *
import logging
logging.getLogger("kafka").addHandler(logging.NullHandler())   # para que kafka-python no llene la pantalla de avisos

# Bytes de control del protocolo con los monitores
ENQ = b'\x05'   # el monitor lo manda al conectarse
ACK = b'\x06'   # respuesta: OK
NACK = b'\x15'  # respuesta: rechazado

# Códigos ANSI para dar color al panel de la terminal
RESET = "\033[0m"
BOLD = "\033[1m"

COLOR_VERDE = "\033[32m"
COLOR_VERDE_PARPADEO = "\033[5;32m"  # verde parpadeante para REGANDO
COLOR_ROJO = "\033[31m"              # rojo para AVERIADA
COLOR_NARANJA = "\033[33m"           # naranja para FUERA DE SERVICIO
COLOR_GRIS = "\033[90m"              # gris para DESCONECTADA


# Devuelve el texto del estado con su color, para el panel de la terminal
def colorear_estado(estado: str) -> str:
    est = (estado or "").upper()
    if est == "DISPONIBLE":
        return f"{COLOR_VERDE}{BOLD}DISPONIBLE{RESET}"
    elif est == "REGANDO":
        return f"{COLOR_VERDE_PARPADEO}{BOLD}REGANDO{RESET}"
    elif est in ("AVERIADA", "FUGA"):
        return f"{COLOR_ROJO}{BOLD}AVERIADA / FUGA{RESET}"
    elif est in ("FUERA DE SERVICIO", "BLOQUEADA"):
        return f"{COLOR_NARANJA}{BOLD}FUERA DE SERVICIO{RESET}"
    elif est == "DESCONECTADA":
        return f"{COLOR_GRIS}{BOLD}DESCONECTADA{RESET}"
    return f"{RESET}{estado}"


# ============================================================
#  SOCKETS CON LOS MONITORES
# ============================================================

# Monitores conectados ahora mismo: {"WS-01": socket, ...}
# Lo usan varios hilos, por eso va con lock
lock_monitores = threading.Lock()
monitores_conectados = {}

# Estaciones que estaban bloqueadas (naranja) cuando les entró una avería.
# Al arreglarse tienen que volver a naranja, no a verde
bloqueadas_con_averia = set()


# Manda una orden (ORDEN#BLOQUEAR, ORDEN#ACTIVAR) al monitor de una estación
def notificar_monitor(ws_id: str, orden: str):
    with lock_monitores:
        sock = monitores_conectados.get(ws_id)
    if sock:
        try:
            sock.sendall(orden.encode("utf-8") + b"\n")
        except Exception:
            pass

TIMEOUT_MONITOR = 6     # si en 6 s no llega nada del monitor (ni un PING), la estación pasa a DESCONECTADA
TIMEOUT_HANDSHAKE = 10  # tiempo máximo para hacer el saludo y el registro


# Qué hacer con cada trama que nos manda un monitor ya registrado
def procesar_trama_monitor(ws_id, texto):
    if texto == "PING":
        return   # solo sirve para saber que el monitor sigue vivo

    if texto == "ALERT#FUGA":
        if get_estado_estacion(ws_id) == "FUERA DE SERVICIO":
            bloqueadas_con_averia.add(ws_id)   # nos acordamos de que estaba bloqueada
        actualizar_estado_estacion(ws_id, "AVERIADA")
        print(f"\n[ALERTA CRÍTICA] ¡Avería/Fuga detectada en {ws_id}! -> Estado en BD: AVERIADA\n")

    elif texto == "ALERT#RESUELTA":
        estado = get_estado_estacion(ws_id)
        if estado == "AVERIADA":
            # Si estaba bloqueada antes de la avería, vuelve a naranja; si no, a verde
            if ws_id in bloqueadas_con_averia:
                bloqueadas_con_averia.discard(ws_id)
                nuevo = "FUERA DE SERVICIO"
            else:
                nuevo = "DISPONIBLE"
            actualizar_estado_estacion(ws_id, nuevo)
            print(f"\n[CENTRAL] Avería resuelta en {ws_id} -> Estado en BD: {nuevo}\n")
        else:
            print(f"[CENTRAL] {ws_id} avisa de avería resuelta, pero está en {estado}: no se cambia.")

    elif texto:
        print(f"[CENTRAL] Trama desconocida de {ws_id}: {texto!r}")


# Atiende a un monitor. Cada monitor que se conecta tiene su propio hilo
# ejecutando esta función. Pasos:
#   1. Saludo: el monitor manda ENQ y le contestamos ACK
#   2. Registro: manda REG#id#ubicacion y lo comprobamos en la BD
#   3. Escucha: recibimos sus PING y sus avisos hasta que se desconecte
def handle_monitor_client(client_socket, client_address):
    ws_id = None
    print(f"\n[CENTRAL] Conexión TCP entrante desde {client_address}")   # client_address = (IP, puerto)
    try:
        # Si en 10 s no completa el saludo y el registro, cerramos: una
        # conexión que no dice nada no puede tener un hilo ocupado para siempre
        client_socket.settimeout(TIMEOUT_HANDSHAKE)

        # --- 1. Saludo ---
        data = client_socket.recv(1024)
        if data != ENQ:
            print(f"[CENTRAL] Byte recibido incorrecto ({data}). Enviando <NACK>...")
            client_socket.sendall(NACK)
            return

        client_socket.sendall(ACK)
        print(f"[CENTRAL] Handshake superado con {client_address}. Esperando registro...")

        # --- 2. Registro: REG#id#ubicacion ---
        reg_data = client_socket.recv(1024).decode('utf-8').strip()
        partes = reg_data.split("#")

        if len(partes) == 3 and partes[0] == "REG":
            ws_id = partes[1]
            ubicacion = partes[2]

            if autenticar_estacion(ws_id, ubicacion):
                with lock_monitores:
                    duplicado = ws_id in monitores_conectados
                    if not duplicado:
                        monitores_conectados[ws_id] = client_socket
                if duplicado:
                    # Ya hay un monitor conectado con este ID: rechazamos al segundo.
                    # Si el primero estaba colgado, a los 6 s sin PING se da por
                    # desconectado y el nuevo podrá entrar en su siguiente intento
                    client_socket.sendall(NACK)
                    print(f"[CENTRAL] Rechazado: ya hay un Monitor conectado como {ws_id}.")
                    return
                actualizar_estado_estacion(ws_id, "DISPONIBLE")
                client_socket.sendall(ACK)
                print(f"[CENTRAL] Estación {ws_id} ({ubicacion}) autenticada con éxito -> Estado: DISPONIBLE")
            else:
                client_socket.sendall(NACK)
                print(f"[CENTRAL] Error: Estación {ws_id} no coincide en BD.")
                return
        else:
            client_socket.sendall(NACK)
            return

        # --- 3. Escucha ---
        # El monitor manda un PING cada 2 s. Si en 6 s no llega nada (monitor
        # colgado, red cortada...), salta el timeout y la damos por desconectada
        client_socket.settimeout(TIMEOUT_MONITOR)
        buffer = ""
        while True:
            try:
                msg = client_socket.recv(1024)
            except socket.timeout:
                print(f"\n[CENTRAL] {ws_id} no da señales de vida en {TIMEOUT_MONITOR}s. Se da por desconectada.")
                break
            if not msg:
                break   # el monitor ha cerrado la conexión

            buffer += msg.decode('utf-8', 'replace')
            # Cada trama acaba en "\n". Puede que lleguen dos juntas o una a
            # medias, así que vamos sacando del buffer las que estén completas
            while "\n" in buffer:
                texto, buffer = buffer.split("\n", 1)
                procesar_trama_monitor(ws_id, texto.strip())

    except Exception as e:
        print(f"[CENTRAL] Incidencia con {ws_id or client_address}: {e}")
    finally:
        # Al salir (por el motivo que sea) quitamos el monitor de la lista y
        # marcamos la estación como DESCONECTADA. Solo si este socket es el
        # que teníamos guardado: si es una conexión rechazada, no tocamos nada
        if ws_id:
            with lock_monitores:
                es_el_actual = monitores_conectados.get(ws_id) is client_socket
                if es_el_actual:
                    monitores_conectados.pop(ws_id, None)
            if es_el_actual:
                actualizar_estado_estacion(ws_id, "DESCONECTADA")
                print(f"[CENTRAL] Estación {ws_id} desconectada -> Estado: DESCONECTADA\n")
        client_socket.close()


# Servidor de sockets: espera conexiones de monitores y lanza un hilo para cada una
def start_socket_server(port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Permite volver a arrancar Central enseguida sin esperar a que se libere el puerto
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # 0.0.0.0 = escuchar en todas las interfaces de red del equipo
    server.bind(("0.0.0.0", port))
    server.listen(50)   # hasta 50 conexiones esperando a ser aceptadas
    print(f"[CENTRAL] Servidor de Sockets a la escucha en el puerto {port}...")

    while True:
        client_sock, addr = server.accept()
        thread = threading.Thread(target=handle_monitor_client, args=(client_sock, addr))
        # daemon = el hilo se cierra solo cuando se cierra Central
        thread.daemon = True
        thread.start()


# ============================================================
#  RIEGOS POR KAFKA
# ============================================================

# Topics (tienen que llamarse igual en WM_FO y WM_WS_E)
TOPIC_SOLICITUDES = "wm_solicitudes"   # Operario/estación -> Central: SOLICITUD, LISTAR
TOPIC_RESPUESTAS = "wm_respuestas"     # Central -> Operario: INFO, AUTORIZADO, DENEGADO, RESUMEN, LISTADO (y el LATIDO)
TOPIC_ORDENES = "wm_ordenes"           # Central -> Engine: ABRIR, CERRAR
TOPIC_TELEMETRIA = "wm_telemetria"     # Engine -> Central/Operario: INICIO, TELEMETRIA, FIN, RECHAZO

TIMEOUT_APERTURA = 5      # segundos que esperamos a que el engine confirme que ha abierto (INICIO)
TIMEOUT_SIN_NOTICIAS = 8  # segundos sin telemetría de un riego en curso -> lo damos por terminado

producer = None
lock_riegos = threading.Lock()   # pendientes y activos los tocan varios hilos
# Riegos que hemos mandado abrir pero el engine todavía no ha confirmado: {ws_id: datos}
pendientes = {}
# Riegos en marcha: {ws_id: {req_id, op_id, caudal, volumen, transcurrido, ultimo, sol}}
activos = {}


# Publica un mensaje en Kafka con la hora y espera a que salga
def enviar(topic, mensaje, clave):
    mensaje["ts"] = time.time()
    producer.send(topic, key=clave, value=mensaje)
    producer.flush(timeout=5)


# Manda un mensaje al operario que hizo la petición 'sol'
def responder_operario(tipo, sol, mensaje, **extra):
    datos = {"tipo": tipo, "op_id": sol.get("op_id"), "req_id": sol.get("req_id"),
             "ws_id": sol.get("ws_id"), "mensaje": mensaje}
    datos.update(extra)
    enviar(TOPIC_RESPUESTAS, datos, clave=sol.get("op_id"))


def denegar(sol, motivo):
    print(f"[CENTRAL-RIEGO] DENEGADO {sol.get('op_id')} -> {sol.get('ws_id')}: {motivo}")
    responder_operario("DENEGADO", sol, motivo)


# Petición de riego de un operario (o de Central, o manual desde la estación).
# Comprobamos operario y estación, la reservamos y le mandamos ABRIR al engine
def procesar_solicitud(sol):
    op_id = sol.get("op_id")
    ws_id = sol.get("ws_id")
    print(f"[CENTRAL-RIEGO] Petición de {op_id}: regar {ws_id} durante {sol.get('duracion')}s")
    responder_operario("INFO", sol, f"CENTRAL ha recibido la petición. Comprobando {ws_id}...")

    # El operario tiene que estar dado de alta (salvo los riegos de Central y los manuales)
    interno = op_id == "CENTRAL" or str(op_id).startswith("MANUAL-")
    if not interno and not existe_operario(op_id):
        return denegar(sol, f"El operario {op_id} no está registrado en CENTRAL")

    # La estación tiene que existir, estar DISPONIBLE y no tener ya otro riego
    estado = get_estado_estacion(ws_id)
    if estado is None:
        return denegar(sol, f"La estación {ws_id} no existe")
    with lock_riegos:
        if ws_id in pendientes or ws_id in activos:
            return denegar(sol, f"La estación {ws_id} ya está ocupada con otro riego")
        if estado != "DISPONIBLE":
            return denegar(sol, f"La estación {ws_id} no está disponible (estado: {estado})")
        # La reservamos para que nadie más la pueda pedir mientras abre
        pendientes[ws_id] = {"enviado": time.time(), "sol": sol}

    responder_operario("INFO", sol, f"{ws_id} disponible. Solicitando apertura de la electroválvula...")
    enviar(TOPIC_ORDENES, {"tipo": "ABRIR", "ws_id": ws_id, "op_id": op_id,
                           "req_id": sol.get("req_id"),
                           "duracion": int(sol.get("duracion", 10))}, clave=ws_id)


# El operario quiere ver todas las estaciones y su estado
def procesar_listado(sol):
    estaciones = [{"ws_id": e[0], "ubicacion": e[1], "estado": e[2]} for e in get_estaciones()]
    responder_operario("LISTADO", sol, "Estado de las estaciones", estaciones=estaciones)


# Mensajes que llegan de los engines por wm_telemetria
def procesar_evento_ws(ev):
    tipo = ev.get("tipo")
    ws_id = ev.get("ws_id")

    # El engine ha abierto la válvula: el riego pasa de pendiente a activo
    # y ahora sí le decimos al operario que está AUTORIZADO
    if tipo == "INICIO":
        with lock_riegos:
            pend = pendientes.get(ws_id)
            if not pend or pend["sol"].get("req_id") != ev.get("req_id"):
                return
            del pendientes[ws_id]
            activos[ws_id] = {"req_id": ev.get("req_id"), "op_id": ev.get("op_id"),
                              "caudal": 0.0, "volumen": 0.0, "transcurrido": 0.0,
                              "ultimo": time.time(), "sol": pend["sol"]}
        actualizar_estado_estacion(ws_id, "REGANDO")
        print(f"[CENTRAL-RIEGO] {ws_id} REGANDO para {ev.get('op_id')}")
        responder_operario("AUTORIZADO", pend["sol"],
                           f"Riego AUTORIZADO. {ws_id} ha abierto la electroválvula.")

    # El engine no ha podido abrir (avería, sin monitor...): se deniega
    elif tipo == "RECHAZO":
        with lock_riegos:
            pend = pendientes.pop(ws_id, None)
        if pend and pend["sol"].get("req_id") == ev.get("req_id"):
            denegar(pend["sol"], f"{ws_id} rechazó la apertura: {ev.get('motivo')}")

    # Datos de caudal y volumen de cada segundo
    elif tipo == "TELEMETRIA":
        with lock_riegos:
            act = activos.get(ws_id)
            if act and act["req_id"] == ev.get("req_id"):
                act["caudal"] = ev.get("caudal")
                act["volumen"] = ev.get("volumen")
                act["transcurrido"] = ev.get("transcurrido")
                act["ultimo"] = time.time()   # última vez que supimos de este riego
        print(f"[CENTRAL-RIEGO] {ws_id} | {ev.get('caudal'):5.2f} l/min | "
              f"{ev.get('volumen'):7.2f} l | operario {ev.get('op_id')}")

    # Fin del riego: ponemos la estación en el estado que toque y mandamos el resumen
    elif tipo == "FIN":
        with lock_riegos:
            act = activos.get(ws_id)
            if act and act["req_id"] != ev.get("req_id"):
                return   # es el FIN atrasado de un riego anterior
            activos.pop(ws_id, None)
            pendientes.pop(ws_id, None)

        motivo = ev.get("motivo")
        estado_actual = get_estado_estacion(ws_id)

        # Si es un riego que no teníamos apuntado (por ejemplo, terminó mientras
        # Central estaba caída) no tocamos el estado, eso ya lo dice el monitor.
        # Solo mandamos el resumen al operario
        if not act:
            pass

        # Ha terminado por una fuga
        elif motivo == "FUGA":
            actualizar_estado_estacion(ws_id, "AVERIADA")

        # Ha terminado porque la hemos bloqueado, o ya estaba en FUERA DE SERVICIO
        elif motivo == "BLOQUEO_ADMIN_CENTRAL" or estado_actual in ("FUERA DE SERVICIO", "BLOQUEADA"):
            actualizar_estado_estacion(ws_id, "FUERA DE SERVICIO")

        # Ha terminado porque se ha caído el monitor
        elif motivo=="MONITOR_CAIDO":
            actualizar_estado_estacion(ws_id, "DESCONECTADA")

        # Fin normal (tiempo límite o parada manual): vuelve a DISPONIBLE
        elif estado_actual == "REGANDO":
            actualizar_estado_estacion(ws_id, "DISPONIBLE")

        print(f"[CENTRAL-RIEGO] {ws_id} FIN de riego ({motivo}): "
              f"{ev.get('duracion')}s, {ev.get('litros')} l")
        sol = act["sol"] if act else {"op_id": ev.get("op_id"), "req_id": ev.get("req_id"),
                                      "ws_id": ws_id}
        responder_operario("RESUMEN", sol, f"Riego finalizado en {ws_id}. Motivo: {motivo}",
                           motivo=motivo, duracion=ev.get("duracion"), litros=ev.get("litros"))


# Hilo que cada segundo revisa dos cosas:
#  - riegos que mandamos abrir y el engine no ha confirmado en 5 s -> se deniegan
#  - riegos en marcha de los que no sabemos nada en 8 s (engine caído o FIN
#    perdido) -> se dan por terminados, para que la estación no se quede
#    ocupada para siempre
def vigilar_timeouts():
    while True:
        time.sleep(1)
        try:
            ahora = time.time()
            with lock_riegos:
                caducados = [(ws, p) for ws, p in pendientes.items()
                             if ahora - p["enviado"] > TIMEOUT_APERTURA]
                for ws, _ in caducados:
                    del pendientes[ws]
                perdidos = [(ws, a) for ws, a in activos.items()
                            if ahora - a["ultimo"] > TIMEOUT_SIN_NOTICIAS]
                for ws, _ in perdidos:
                    del activos[ws]
            for ws, p in caducados:
                denegar(p["sol"], f"{ws} no ha respondido a la orden de apertura")
            for ws, a in perdidos:
                if get_estado_estacion(ws) == "REGANDO":
                    actualizar_estado_estacion(ws, "DISPONIBLE")
                print(f"[CENTRAL-RIEGO] {ws}: sin noticias del riego en {TIMEOUT_SIN_NOTICIAS}s. Se da por terminado.")
                responder_operario("RESUMEN", a["sol"], f"Sin noticias de {ws}: riego dado por terminado",
                                   motivo="SIN_NOTICIAS_ESTACION",
                                   duracion=a["transcurrido"], litros=a["volumen"])
        except Exception as e:
            # Si Kafka está caído, enviar() da error. Con el try el hilo sigue
            # vivo y lo vuelve a intentar en la siguiente vuelta
            print(f"[CENTRAL-RIEGO] Error vigilando timeouts (se reintenta): {e}")


# Al arrancar Central nos saltamos las peticiones que se quedaron en Kafka
# mientras estaba apagada (los operarios ya las habrán dado por perdidas).
# No miramos la hora de los mensajes porque la pone el ordenador que los
# envía, y en el laboratorio había PCs con el reloj desfasado una hora.
# La telemetría NO se salta: si un riego terminó con Central caída, su FIN
# está ahí y hay que procesarlo para mandarle el resumen al operario
def saltar_mensajes_antiguos(consumer):
    telemetria_previa = []
    limite = time.time() + 30
    # Esperamos a que Kafka nos asigne los topics
    while not consumer.assignment() and time.time() < limite:
        lotes = consumer.poll(timeout_ms=200)
        for registros in lotes.values():
            telemetria_previa += [r for r in registros if r.topic == TOPIC_TELEMETRIA]
    # Solo el topic de peticiones lo movemos al final
    solicitudes = [tp for tp in consumer.assignment() if tp.topic == TOPIC_SOLICITUDES]
    if solicitudes:
        consumer.seek_to_end(*solicitudes)
        for tp in solicitudes:
            consumer.position(tp)
    return telemetria_previa


# Hilo que lee de Kafka. Usamos un solo consumidor para peticiones y
# telemetría: así todo se procesa en orden y en un único hilo, y dos
# peticiones a la misma estación nunca se cruzan
def bucle_kafka(broker):
    consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        group_id="wm_central",
        auto_offset_reset="latest",
        metadata_max_age_ms=3000,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    consumer.subscribe([TOPIC_SOLICITUDES, TOPIC_TELEMETRIA])
    for msg in saltar_mensajes_antiguos(consumer):
        try:
            procesar_evento_ws(msg.value)
        except Exception as e:
            print(f"[CENTRAL-RIEGO] Error procesando {msg.value}: {e}")
    print(f"[CENTRAL-RIEGO] Escuchando '{TOPIC_SOLICITUDES}' y '{TOPIC_TELEMETRIA}' en {broker}")
    while True:
        try:
            lotes = consumer.poll(timeout_ms=500)
        except Exception as e:   # si Kafka se cae, esperamos y volvemos a intentar
            print(f"[CENTRAL-RIEGO] Error leyendo de Kafka (se reintenta): {e}")
            time.sleep(2)
            continue
        for registros in lotes.values():
            for msg in registros:
                m = msg.value
                try:
                    if msg.topic == TOPIC_SOLICITUDES:
                        if m.get("tipo") == "SOLICITUD":
                            procesar_solicitud(m)
                        elif m.get("tipo") == "LISTAR":
                            procesar_listado(m)
                    else:
                        procesar_evento_ws(m)
                except Exception as e:   # un mensaje mal formado no puede tirar Central
                    print(f"[CENTRAL-RIEGO] Error procesando {m}: {e}")


# Crea los topics si no existen. Si falla no pasa nada: Kafka los crea solo
def crear_topics(broker):
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        admin = KafkaAdminClient(bootstrap_servers=[broker])
        existentes = set(admin.list_topics())
        nuevos = [NewTopic(name=t, num_partitions=1, replication_factor=1)
                  for t in (TOPIC_SOLICITUDES, TOPIC_RESPUESTAS, TOPIC_ORDENES, TOPIC_TELEMETRIA)
                  if t not in existentes]
        if nuevos:
            admin.create_topics(nuevos)
        admin.close()
    except Exception as e:
        print(f"[CENTRAL-RIEGO] Aviso: no se pudieron crear los topics ({e})")


# Hilo que publica un LATIDO cada 2 s. Los operarios y los engines lo usan
# para saber si Central sigue viva
def latido():
    while True:
        try:
            producer.send(TOPIC_RESPUESTAS, key="CENTRAL", value={"tipo": "LATIDO", "ts": time.time()})
        except Exception:
            pass
        time.sleep(2)


# Conecta con Kafka y arranca los tres hilos de los riegos
def iniciar_kafka(broker):
    global producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
        )
    except NoBrokersAvailable:
        print(f"[CENTRAL-RIEGO] AVISO: no hay Kafka en {broker}. Central funcionará solo con sockets.")
        return
    crear_topics(broker)
    threading.Thread(target=bucle_kafka, args=(broker,), daemon=True).start()
    threading.Thread(target=vigilar_timeouts, daemon=True).start()
    threading.Thread(target=latido, daemon=True).start()


# ============================================================
#  ÓRDENES DE ADMINISTRACIÓN (punto 11 del enunciado)
#  Se usan igual desde la consola y desde el panel web.
#  ws_id puede ser una estación o "ALL" para todas
# ============================================================

# Iniciar un riego desde Central: se manda una SOLICITUD como si fuéramos un
# operario (con op_id "CENTRAL"), así pasa por las mismas comprobaciones
def admin_forzar_riego(ws_id: str, duracion: int = 20, publicar_mensaje=None):
    estaciones = get_estaciones()
    destinos = [e[0] for e in estaciones] if ws_id.lower() == "all" else [ws_id]

    for target in destinos:
        estado = get_estado_estacion(target)
        if estado != "DISPONIBLE":
            mensaje = (
                f"[ADMIN] Rechazado: {target} no está DISPONIBLE "
                f"(Estado actual: {estado})"
            )
            if publicar_mensaje:
                publicar_mensaje(mensaje)
            else:
                print(mensaje)
            continue

        sol = {
            "tipo": "SOLICITUD",
            "req_id": f"CEN-{uuid.uuid4().hex[:8]}",
            "op_id": "CENTRAL",
            "ws_id": target,
            "duracion": duracion
        }
        enviar(TOPIC_SOLICITUDES, sol, clave="CENTRAL")
        mensaje = f"[ADMIN] Orden de riego enviada para {target} ({duracion}s)"
        if publicar_mensaje:
            publicar_mensaje(mensaje)
        else:
            print(mensaje)


# Bloquear: pasa a FUERA DE SERVICIO (naranja) y corta el riego si lo hay.
#  - DISPONIBLE o REGANDO -> se bloquea
#  - AVERIADA -> sigue en rojo, pero cuando se arregle quedará en naranja
#  - DESCONECTADA o no existe -> no se toca
# Devuelve un texto con lo que ha pasado para enseñarlo en pantalla
def admin_bloquear_estacion(ws_id: str):
    estaciones = get_estaciones()
    destinos = [e[0] for e in estaciones] if ws_id.lower() == "all" else [ws_id]

    bloqueadas, en_averia, rechazadas = [], [], []
    for target in destinos:
        estado = get_estado_estacion(target)

        if estado in ("DISPONIBLE", "REGANDO"):
            with lock_riegos:
                riego_activo = target in activos
            if estado == "REGANDO" or riego_activo:
                ordenar_cierre(target, motivo="BLOQUEO_ADMIN_CENTRAL")
            actualizar_estado_estacion(target, "FUERA DE SERVICIO")
            notificar_monitor(target, "ORDEN#BLOQUEAR")
            bloqueadas.append(target)

        elif estado == "AVERIADA":
            bloqueadas_con_averia.add(target)
            notificar_monitor(target, "ORDEN#BLOQUEAR")
            en_averia.append(target)

        else:
            rechazadas.append(f"{target} ({estado or 'no existe'})")

    resultado = []
    if bloqueadas:
        resultado.append(f"Bloqueadas: {', '.join(bloqueadas)}.")
    if en_averia:
        resultado.append(f"Siguen en ROJO por avería; quedarán bloqueadas al repararse: {', '.join(en_averia)}.")
    if rechazadas:
        resultado.append(f"No se bloquean: {', '.join(rechazadas)}.")
    return "[ADMIN] " + " ".join(resultado)


# Activar: solo para las que están en FUERA DE SERVICIO, que vuelven a DISPONIBLE.
# Las demás (averiadas, desconectadas...) no se tocan
def admin_activar_estacion(ws_id: str):
    estaciones = get_estaciones()
    destinos = [e[0] for e in estaciones] if ws_id.lower() == "all" else [ws_id]

    activadas, rechazadas = [], []
    for target in destinos:
        estado = get_estado_estacion(target)
        if estado != "FUERA DE SERVICIO":
            rechazadas.append(f"{target} ({estado or 'no existe'})")
            continue

        bloqueadas_con_averia.discard(target)
        actualizar_estado_estacion(target, "DISPONIBLE")
        notificar_monitor(target, "ORDEN#ACTIVAR")
        activadas.append(target)

    resultado = []
    if activadas:
        resultado.append(f"Activadas: {', '.join(activadas)}.")
    if rechazadas:
        resultado.append(f"No se activan (no están FUERA DE SERVICIO): {', '.join(rechazadas)}.")
    return "[ADMIN] " + " ".join(resultado)


# Le dice al engine que cierre la válvula (por ejemplo, al bloquear la estación)
def ordenar_cierre(ws_id, motivo="ORDEN_CENTRAL"):
    enviar(TOPIC_ORDENES, {"tipo": "CERRAR", "ws_id": ws_id, "motivo": motivo}, clave=ws_id)


# Datos de los riegos en marcha para pintarlos en los paneles
def obtener_telemetria():
    with lock_riegos:
        return {ws: {k: v for k, v in d.items() if k not in ("sol", "ultimo")}
                for ws, d in activos.items()}


# ============================================================
#  PANEL DE LA TERMINAL Y CONSOLA DE ADMINISTRACIÓN
# ============================================================

# Construye el texto del panel. Se vuelve a pintar cada segundo
def construir_panel():
    estaciones_bd = get_estaciones()
    telemetria_viva = obtener_telemetria()

    lineas = []
    lineas.append("=================================================================================================================")
    lineas.append(f" {BOLD}WM_CENTRAL - PANEL DE CONTROL Y MONITORIZACIÓN EN TIEMPO REAL{RESET}")
    lineas.append(f" Última actualización: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lineas.append("=================================================================================================================")
    lineas.append(f"{'ID':<10} | {'UBICACIÓN':<22} | {'ESTADO':<30} | {'CAUDAL (l/min)':<14} | {'VOLUMEN (l)':<12} | {'OPERARIO':<15}")
    lineas.append("-----------------------------------------------------------------------------------------------------------------")

    if not estaciones_bd:
        lineas.append("  No hay estaciones registradas en la base de datos.")
    else:
        for est in estaciones_bd:
            ws_id, ubicacion, estado = est[0], est[1], est[2]

            # El caudal, el volumen y el operario solo se enseñan mientras riega
            tele = telemetria_viva.get(ws_id, {})
            caudal = tele.get("caudal", 0.0) if estado == "REGANDO" else 0.0
            volumen = tele.get("volumen", 0.0) if estado == "REGANDO" else 0.0
            operario = tele.get("op_id", "-") if (estado == "REGANDO" and tele) else "-"

            estado_fmt = colorear_estado(estado)
            lineas.append(f"{ws_id:<10} | {ubicacion:<22} | {estado_fmt:<40} | {caudal:>10.2f}     | {volumen:>8.2f}     | {operario:<15}")

    lineas.append("=================================================================================================================")
    lineas.append(f" LEYENDA: {COLOR_VERDE}● Disponible{RESET}  {COLOR_VERDE}● Regando{RESET}  {COLOR_ROJO}● Averiada/Fuga{RESET}  {COLOR_NARANJA}● Fuera de Servicio{RESET}  {COLOR_GRIS}● Desconectada{RESET}")
    lineas.append("=================================================================================================================")
    lineas.append(" COMANDOS: regar <ID|all> <seg>  |  bloquear <ID|all>  |  activar <ID|all>")
    lineas.append("           alta-ws <ID> <ubicación>  |  alta-fo <ID> <nombre>")
    return "\n".join(lineas)


# Formato válido de un ID: letras mayúsculas, números, guion y guion bajo (máx. 20)
ID_VALIDO = re.compile(r"^[A-Z0-9_-]{1,20}$")


# Alta de una estación. La ubicación tiene que ser exactamente la misma que
# luego se le pase al monitor, si no Central lo rechazará. No puede llevar
# '#' porque es el separador de la trama REG#id#ubicacion
def admin_alta_estacion(ws_id: str, ubicacion: str) -> str:
    ws_id = ws_id.strip().upper()
    ubicacion = ubicacion.strip().strip('"').strip("'").strip()
    if not ID_VALIDO.match(ws_id) or ws_id == "ALL":
        return "[ADMIN] ID de estación no válido (letras, números, - o _; máx. 20)."
    if not ubicacion or len(ubicacion) > 50 or "#" in ubicacion:
        return "[ADMIN] Ubicación no válida (1-50 caracteres y sin '#')."
    resultado = alta_estacion(ws_id, ubicacion)
    return f"[ADMIN] Estación {ws_id} ({ubicacion}) {resultado}. Arranca su Monitor con esa misma ubicación."


# Alta de un operario. CENTRAL y MANUAL-... no se permiten porque son los
# op_id que usamos para los riegos de Central y los manuales de la estación
def admin_alta_operario(op_id: str, nombre: str) -> str:
    op_id = op_id.strip().upper()
    nombre = nombre.strip().strip('"').strip("'").strip()
    if not ID_VALIDO.match(op_id) or op_id == "CENTRAL" or op_id.startswith("MANUAL-"):
        return "[ADMIN] ID de operario no válido (letras, números, - o _; máx. 20)."
    if not nombre or len(nombre) > 50:
        return "[ADMIN] Nombre no válido (1-50 caracteres)."
    resultado = alta_operario(op_id, nombre)
    return f"[ADMIN] Operario {op_id} ({nombre}) {resultado}."


# Interpreta un comando escrito en la consola (o enviado desde la web) y
# llama a la función que toque. El resultado se pasa a publicar_mensaje
def ejecutar_comando_admin(command: str, publicar_mensaje):
    try:
        partes = command.split()
        if not partes:
            return

        accion = partes[0].lower()
        if accion == "regar":
            if len(partes) != 3:
                publicar_mensaje("[ADMIN] Uso: regar <WS_ID|all> <segundos>")
                return
            admin_forzar_riego(partes[1].upper(), int(partes[2]), publicar_mensaje)
        elif accion == "bloquear" and len(partes) == 2:
            publicar_mensaje(admin_bloquear_estacion(partes[1].upper()))
        elif accion == "activar" and len(partes) == 2:
            publicar_mensaje(admin_activar_estacion(partes[1].upper()))
        elif accion == "alta-ws" and len(partes) >= 3:
            # La ubicación puede tener espacios ("Parque Norte"), por eso juntamos el resto
            publicar_mensaje(admin_alta_estacion(partes[1], " ".join(partes[2:])))
        elif accion == "alta-fo" and len(partes) >= 3:
            publicar_mensaje(admin_alta_operario(partes[1], " ".join(partes[2:])))
        else:
            publicar_mensaje(
                "[ADMIN] Uso: regar <WS_ID|all> <segundos>, "
                "bloquear <WS_ID|all>, activar <WS_ID|all>, "
                "alta-ws <WS_ID> <ubicación>, alta-fo <FO_ID> <nombre>"
            )
    except Exception as exc:
        publicar_mensaje(f"[ADMIN ERROR] {exc}")


# Panel de la terminal (solo cuando Central se lanza en una terminal, en local).
# Está hecho con prompt_toolkit: arriba el panel, que se repinta cada segundo,
# una línea con la respuesta al último comando y abajo donde se escribe
def ejecutar_consola_admin():
    # El panel tiene que dibujarse directamente en la terminal. Si no, todo
    # lo que dibuja acabaría también en los mensajes del panel web
    terminal_real = sys.stdout.destino if isinstance(sys.stdout, SalidaConRegistro) else sys.stdout
    sys.stdout = terminal_real
    entrada = TextArea(height=1, prompt="> ", multiline=False)
    atajos = KeyBindings()
    mensaje_admin = [" "]           # respuesta al último comando
    lock_mensaje = threading.Lock()
    app_ref = [None]

    def publicar_mensaje(mensaje):
        with lock_mensaje:
            mensaje_admin[0] = mensaje
        if app_ref[0] is not None:
            app_ref[0].invalidate()   # repintar ya, sin esperar al siguiente segundo

    def leer_mensaje():
        with lock_mensaje:
            return mensaje_admin[0]

    @atajos.add("c-c")
    def salir(event):
        event.app.exit()

    # Al pulsar Enter, el comando se ejecuta en otro hilo para que el panel no se congele
    @atajos.add("enter")
    def enviar_comando(_event):
        command = entrada.text.strip()
        entrada.text = ""
        if command:
            publicar_mensaje("[ADMIN] Procesando comando...")
            threading.Thread(
                target=ejecutar_comando_admin,
                args=(command, publicar_mensaje),
                daemon=True,
            ).start()

    panel = Window(
        content=FormattedTextControl(lambda: ANSI(construir_panel())),
        height=Dimension(weight=1),
        wrap_lines=False,
    )
    estado_admin = Window(
        content=FormattedTextControl(leer_mensaje),
        height=1,
        wrap_lines=False,
    )
    app = Application(
        layout=Layout(HSplit([panel, estado_admin, entrada]), focused_element=entrada),
        key_bindings=atajos,
        full_screen=True,
        refresh_interval=1.0,
    )
    app_ref[0] = app

    try:
        with patch_stdout():
            # patch_stdout cambia sys.stdout. Lo envolvemos otra vez para que los
            # mensajes sigan llegando al panel web mientras la consola está abierta
            sys.stdout = SalidaConRegistro(sys.stdout)
            app.run()
    finally:
        sys.stdout = SalidaConRegistro(terminal_real)


# ============================================================
#  PANEL WEB
#  En Railway no hay terminal, así que Central también sirve una página web.
#  La página pide el estado cada segundo a /api/estado y manda los botones
#  a /api/comando, que usa las mismas funciones que la consola.
#  Si existe la variable WM_PANEL_PASSWORD, la página pide contraseña.
# ============================================================

# Últimos mensajes de la aplicación, para enseñarlos en la web
lock_log = threading.Lock()
log_mensajes = collections.deque(maxlen=60)   # deque con maxlen: al llegar a 60 se van borrando los más viejos


# Sustituye a sys.stdout. Cada print de cualquier hilo se sigue escribiendo
# donde iba (terminal o logs de Railway) y además se guarda en log_mensajes
# para que la web lo pueda enseñar
class SalidaConRegistro:
    def __init__(self, destino):
        self.destino = destino
        self.pendiente = ""   # trozo de línea que todavía no ha terminado en "\n"

    def write(self, texto):
        try:
            self.destino.write(texto)
        except Exception:
            pass
        self.pendiente += texto
        while "\n" in self.pendiente:
            linea, self.pendiente = self.pendiente.split("\n", 1)
            linea = linea.strip()
            if linea and not linea.startswith(("=====", "-----")):   # las líneas de adorno no
                with lock_log:
                    log_mensajes.append(f"[{time.strftime('%H:%M:%S')}] {linea}")
        return len(texto)

    def flush(self):
        try:
            self.destino.flush()
        except Exception:
            pass

    def isatty(self):
        try:
            return self.destino.isatty()
        except Exception:
            return False

    # Cualquier otra cosa que se le pida (encoding, fileno...) la da el stdout real
    def __getattr__(self, nombre):
        return getattr(self.destino, nombre)


# Lo que devuelve /api/estado: estaciones con su telemetría y los últimos mensajes
def estado_para_web():
    telemetria = obtener_telemetria()
    estaciones = []
    for ws_id, ubicacion, estado in get_estaciones():
        tele = telemetria.get(ws_id, {}) if estado == "REGANDO" else {}
        estaciones.append({
            "id": ws_id, "ubicacion": ubicacion, "estado": estado,
            "caudal": round(float(tele.get("caudal") or 0.0), 2),
            "volumen": round(float(tele.get("volumen") or 0.0), 2),
            "operario": tele.get("op_id") or "-",
        })
    with lock_log:
        mensajes = list(log_mensajes)[-25:]
    return {"hora": time.strftime("%Y-%m-%d %H:%M:%S"),
            "kafka": producer is not None,
            "estaciones": estaciones, "mensajes": mensajes}


# Convierte el botón que se ha pulsado en la web en un comando de la consola
# y devuelve la respuesta para enseñarla en la página
def ejecutar_comando_web(datos):
    accion = str(datos.get("accion", "")).lower()
    if accion == "alta-ws":
        respuesta = admin_alta_estacion(str(datos.get("id", "")), str(datos.get("texto", "")))
        print(f"[WEB] {respuesta}")
        return [respuesta]
    if accion == "alta-fo":
        respuesta = admin_alta_operario(str(datos.get("id", "")), str(datos.get("texto", "")))
        print(f"[WEB] {respuesta}")
        return [respuesta]
    ws_id = str(datos.get("ws_id", "")).strip().upper() or "ALL"
    if accion not in ("regar", "bloquear", "activar"):
        return ["[ADMIN] Acción no válida."]
    if ws_id != "ALL" and get_estado_estacion(ws_id) is None:
        return [f"[ADMIN] La estación {ws_id} no existe."]
    comando = f"{accion} {ws_id}"
    if accion == "regar":
        try:
            segundos = int(datos.get("segundos", 20))
        except (TypeError, ValueError):
            return ["[ADMIN] Los segundos deben ser un número."]
        if not 1 <= segundos <= 3600:
            return ["[ADMIN] Los segundos deben estar entre 1 y 3600."]
        comando += f" {segundos}"
    respuestas = []
    ejecutar_comando_admin(comando, respuestas.append)
    for r in respuestas:
        print(f"[WEB] {r}")
    return respuestas or [f"[ADMIN] Comando '{comando}' ejecutado."]


# Página del panel web (HTML, CSS y JavaScript). Es un texto que el servidor
# le manda al navegador tal cual
PAGINA_PANEL = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WaterManagement · Central</title>
<style>
  :root { --fondo:#0b1418; --panel:#11202a; --borde:#1f3542; --texto:#d7e6ec; --suave:#7f99a6;
          --verde:#34c77b; --rojo:#ef5350; --naranja:#f39c35; --gris:#7d8b93; --acento:#4fb3d9; }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--fondo); color:var(--texto);
         font-family:"Segoe UI",system-ui,-apple-system,sans-serif; }
  header { display:flex; flex-wrap:wrap; gap:12px; align-items:center; justify-content:space-between;
           padding:16px 22px; border-bottom:1px solid var(--borde); }
  .marca small { display:block; color:var(--suave); font-size:11px; letter-spacing:2px; }
  .marca b { font-size:20px; } .marca span { color:var(--suave); font-weight:400; }
  .resumen { display:flex; flex-wrap:wrap; gap:8px; }
  .chip { padding:4px 11px; border-radius:999px; border:1px solid var(--borde); font-size:13px; }
  .chip i { display:inline-block; width:9px; height:9px; border-radius:50%; margin-right:6px; }
  main { display:grid; grid-template-columns:minmax(0,1fr) 360px; gap:18px; padding:18px 22px; }
  @media (max-width:900px){ main { grid-template-columns:1fr; } }
  .caja { background:var(--panel); border:1px solid var(--borde); border-radius:12px; padding:14px; }
  .caja h2 { margin:0 0 12px; font-size:12px; letter-spacing:2px; color:var(--suave); font-weight:600; }
  .estaciones { display:grid; grid-template-columns:repeat(auto-fill,minmax(250px,1fr)); gap:12px; }
  .ws { border:1px solid var(--borde); border-left:6px solid var(--gris); border-radius:10px;
        padding:12px; background:#0e1b23; }
  .ws .cab { display:flex; justify-content:space-between; align-items:baseline; }
  .ws .id { font-weight:700; font-size:16px; } .ws .ubi { color:var(--suave); font-size:13px; }
  .estado { display:inline-block; margin:8px 0; padding:3px 10px; border-radius:6px; font-weight:700;
            font-size:12px; letter-spacing:1px; color:#0b1418; background:var(--gris); }
  .datos { display:grid; grid-template-columns:repeat(3,1fr); gap:6px; font-size:12px; color:var(--suave); }
  .datos b { display:block; color:var(--texto); font-size:15px; }
  .acciones { display:flex; flex-wrap:wrap; gap:6px; margin-top:10px; align-items:center; }
  button { cursor:pointer; border:1px solid var(--borde); background:#16303d; color:var(--texto);
           border-radius:7px; padding:6px 10px; font-size:13px; }
  button:hover { border-color:var(--acento); } button:disabled { opacity:.4; cursor:not-allowed; }
  button.regar { border-color:#246b48; } button.bloquear { border-color:#7a4d1d; } button.activar { border-color:#246b48; }
  input[type=text] { background:#0b1418; color:var(--texto); border:1px solid var(--borde);
                     border-radius:7px; padding:6px; font-size:13px; max-width:100%; }
  input[type=number] { width:62px; background:#0b1418; color:var(--texto); border:1px solid var(--borde);
                       border-radius:7px; padding:6px; font-size:13px; }
  .DISPONIBLE { border-left-color:var(--verde); } .DISPONIBLE .estado { background:var(--verde); }
  .REGANDO { border-left-color:var(--verde); } .REGANDO .estado { background:var(--verde); animation:parpadeo 1s steps(2,start) infinite; }
  .REGANDO { box-shadow:0 0 0 1px rgba(52,199,123,.35); }
  .AVERIADA { border-left-color:var(--rojo); } .AVERIADA .estado { background:var(--rojo); }
  .FUERA { border-left-color:var(--naranja); } .FUERA .estado { background:var(--naranja); }
  .DESCONECTADA .estado { background:var(--gris); }
  @keyframes parpadeo { to { opacity:.25; } }
  .todas { display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin-bottom:12px; }
  #mensajes { font-family:ui-monospace,Menlo,Consolas,monospace; font-size:12px; line-height:1.5;
              max-height:60vh; overflow-y:auto; white-space:pre-wrap; word-break:break-word; }
  #mensajes div { border-bottom:1px solid #15262f; padding:2px 0; }
  .alerta { color:#ff8a80; } .ok { color:#8ee0b0; } .admin { color:#9fd3ea; }
  #respuesta { min-height:20px; font-size:13px; color:#9fd3ea; margin-bottom:10px; }
  footer { padding:0 22px 18px; color:var(--suave); font-size:12px; }
  .desconexion { color:var(--rojo) !important; font-weight:700; }
</style>
</head>
<body>
<header>
  <div class="marca"><small>SAMPLE CITY · PARKS &amp; GARDENS NETWORK</small>
    <b>WaterManagement <span>· Panel de control de Central</span></b></div>
  <div class="resumen" id="resumen"></div>
</header>
<main>
  <section class="caja">
    <h2>ESTACIONES DE RIEGO</h2>
    <div class="todas">
      <span>Todas las estaciones:</span>
      <input type="number" id="seg-all" min="1" max="3600" value="20" title="segundos">
      <button class="regar" onclick="orden('regar','ALL','seg-all')">💧 Regar todas</button>
      <button class="bloquear" onclick="orden('bloquear','ALL')">⛔ Bloquear todas</button>
      <button class="activar" onclick="orden('activar','ALL')">✅ Activar todas</button>
    </div>
    <div id="respuesta"></div>
    <div class="estaciones" id="estaciones"></div>
    <h2 style="margin-top:18px">DAR DE ALTA</h2>
    <div class="todas">
      <span>Estación:</span>
      <input type="text" id="alta-ws-id" placeholder="WS-05" size="8">
      <input type="text" id="alta-ws-texto" placeholder="Ubicación (ej. Parque Norte)" size="24">
      <button onclick="alta('alta-ws','alta-ws-id','alta-ws-texto')">➕ Alta estación</button>
    </div>
    <div class="todas">
      <span>Operario:</span>
      <input type="text" id="alta-fo-id" placeholder="FO-03" size="8">
      <input type="text" id="alta-fo-texto" placeholder="Nombre" size="24">
      <button onclick="alta('alta-fo','alta-fo-id','alta-fo-texto')">➕ Alta operario</button>
    </div>
  </section>
  <aside class="caja">
    <h2>MENSAJES DE LA APLICACIÓN</h2>
    <div id="mensajes"></div>
  </aside>
</main>
<footer id="pie">Conectando con Central…</footer>
<script>
const COLORES = {DISPONIBLE:"var(--verde)", REGANDO:"var(--verde)", AVERIADA:"var(--rojo)",
                 "FUERA DE SERVICIO":"var(--naranja)", DESCONECTADA:"var(--gris)"};
const TEXTO = {DISPONIBLE:"DISPONIBLE", REGANDO:"REGANDO", AVERIADA:"AVERIADA / FUGA",
               "FUERA DE SERVICIO":"FUERA DE SERVICIO", DESCONECTADA:"DESCONECTADA"};
const segundos = {};   // conserva lo que el usuario escribe en cada estación entre refrescos

function clase(estado){ return estado === "FUERA DE SERVICIO" ? "FUERA" : estado; }

function el(tag, attrs = {}, hijos = []) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "text") e.textContent = v; else if (k.startsWith("on")) e[k] = v; else e.setAttribute(k, v);
  }
  hijos.forEach(h => e.appendChild(h));
  return e;
}

function pintar(datos) {
  // Resumen por estado
  const cuenta = {};
  datos.estaciones.forEach(s => cuenta[s.estado] = (cuenta[s.estado] || 0) + 1);
  const resumen = document.getElementById("resumen"); resumen.replaceChildren();
  Object.keys(TEXTO).forEach(est => {
    const chip = el("span", {class:"chip"}, [el("i", {style:`background:${COLORES[est]}`})]);
    chip.appendChild(document.createTextNode(`${cuenta[est] || 0} ${TEXTO[est].toLowerCase()}`));
    resumen.appendChild(chip);
  });

  // Tarjetas de estaciones
  const cont = document.getElementById("estaciones");
  const enfocado = document.activeElement && document.activeElement.id;
  cont.replaceChildren();
  datos.estaciones.forEach(s => {
    const idSeg = "seg-" + s.id;
    const regando = s.estado === "REGANDO";
    const input = el("input", {type:"number", id:idSeg, min:"1", max:"3600", title:"segundos"});
    input.value = segundos[s.id] || 20;
    input.oninput = () => segundos[s.id] = input.value;
    const tarjeta = el("div", {class:"ws " + clase(s.estado)}, [
      el("div", {class:"cab"}, [el("span", {class:"id", text:s.id}), el("span", {class:"ubi", text:s.ubicacion})]),
      el("span", {class:"estado", text:TEXTO[s.estado] || s.estado}),
      el("div", {class:"datos"}, [
        el("div", {}, [document.createTextNode("Caudal"), el("b", {text: s.caudal.toFixed(2) + " l/min"})]),
        el("div", {}, [document.createTextNode("Volumen"), el("b", {text: s.volumen.toFixed(2) + " l"})]),
        el("div", {}, [document.createTextNode("Operario"), el("b", {text: s.operario})]),
      ]),
      el("div", {class:"acciones"}, [
        input,
        el("button", {class:"regar", onclick: () => orden("regar", s.id, idSeg)}, [document.createTextNode("💧 Regar")]),
        el("button", {class:"bloquear", onclick: () => orden("bloquear", s.id)}, [document.createTextNode("⛔ Bloquear")]),
        el("button", {class:"activar", onclick: () => orden("activar", s.id)}, [document.createTextNode("✅ Activar")]),
      ]),
    ]);
    cont.appendChild(tarjeta);
  });
  if (enfocado && document.getElementById(enfocado)) document.getElementById(enfocado).focus();

  // Mensajes (los más recientes arriba)
  const caja = document.getElementById("mensajes"); caja.replaceChildren();
  datos.mensajes.slice().reverse().forEach(m => {
    let c = "";
    if (/ALERTA|DENEGADO|desconectad|Error|✖/i.test(m)) c = "alerta";
    else if (/autenticada|resuelta|AUTORIZADO|REGANDO/i.test(m)) c = "ok";
    else if (/ADMIN|WEB/.test(m)) c = "admin";
    caja.appendChild(el("div", {class:c, text:m}));
  });

  document.getElementById("pie").className = "";
  document.getElementById("pie").textContent =
    `Última actualización: ${datos.hora} · Kafka: ${datos.kafka ? "conectado" : "NO disponible"} · se refresca cada segundo`;
}

async function refrescar() {
  try {
    const r = await fetch("/api/estado", {cache:"no-store"});
    if (!r.ok) throw new Error(r.status);
    pintar(await r.json());
  } catch (e) {
    const pie = document.getElementById("pie");
    pie.className = "desconexion";
    pie.textContent = "✖ Sin conexión con WM_Central. Reintentando…";
  }
}

async function orden(accion, ws_id, idSegundos) {
  const cuerpo = {accion, ws_id};
  if (idSegundos) cuerpo.segundos = Number(document.getElementById(idSegundos).value || 20);
  const resp = document.getElementById("respuesta");
  resp.textContent = `Enviando '${accion}' a ${ws_id === "ALL" ? "todas" : ws_id}…`;
  try {
    const r = await fetch("/api/comando", {method:"POST", headers:{"Content-Type":"application/json"},
                                           body: JSON.stringify(cuerpo)});
    const datos = await r.json();
    resp.textContent = (datos.mensajes || []).join("  ·  ");
  } catch (e) {
    resp.textContent = "✖ No se pudo enviar la orden a Central.";
  }
  refrescar();
}

async function alta(accion, idCampo, idTexto) {
  const cuerpo = {accion, id: document.getElementById(idCampo).value,
                  texto: document.getElementById(idTexto).value};
  const resp = document.getElementById("respuesta");
  try {
    const r = await fetch("/api/comando", {method:"POST", headers:{"Content-Type":"application/json"},
                                           body: JSON.stringify(cuerpo)});
    const datos = await r.json();
    resp.textContent = (datos.mensajes || []).join("  ·  ");
    if (/creada|creado|actualizada|actualizado/.test(resp.textContent)) {
      document.getElementById(idCampo).value = ""; document.getElementById(idTexto).value = "";
    }
  } catch (e) {
    resp.textContent = "✖ No se pudo enviar el alta a Central.";
  }
  refrescar();
}

refrescar();
setInterval(refrescar, 1000);
</script>
</body>
</html>
"""


# Atiende las peticiones del navegador:
#   GET  /             -> la página
#   GET  /api/estado   -> el estado en JSON
#   POST /api/comando  -> ejecutar un botón
class ManejadorPanelWeb(http.server.BaseHTTPRequestHandler):
    def log_message(self, formato, *args):
        pass   # sin esto, cada petición (una por segundo) llenaría los logs

    # Si hay contraseña, el navegador la manda en la cabecera Authorization
    # (codificada en base64). Si no coincide, devolvemos 401 y el navegador la pide
    def _autorizado(self):
        clave = os.environ.get("WM_PANEL_PASSWORD")
        if not clave:
            return True
        cabecera = self.headers.get("Authorization", "")
        if cabecera.startswith("Basic "):
            try:
                _, _, enviada = base64.b64decode(cabecera[6:]).decode("utf-8").partition(":")
                if hmac.compare_digest(enviada, clave):
                    return True
            except Exception:
                pass
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="WM_Central"')
        self.end_headers()
        return False

    def _responder(self, codigo, tipo, cuerpo):
        datos = cuerpo.encode("utf-8")
        self.send_response(codigo)
        self.send_header("Content-Type", tipo)
        self.send_header("Content-Length", str(len(datos)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(datos)

    def do_GET(self):
        if not self._autorizado():
            return
        if self.path in ("/", "/index.html"):
            self._responder(200, "text/html; charset=utf-8", PAGINA_PANEL)
        elif self.path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
        elif self.path.startswith("/api/estado"):
            self._responder(200, "application/json", json.dumps(estado_para_web()))
        else:
            self._responder(404, "text/plain; charset=utf-8", "No encontrado")

    def do_POST(self):
        if not self._autorizado():
            return
        if self.path != "/api/comando":
            self._responder(404, "text/plain; charset=utf-8", "No encontrado")
            return
        try:
            longitud = min(int(self.headers.get("Content-Length", 0)), 10_000)   # como mucho leemos 10 KB
            datos = json.loads(self.rfile.read(longitud) or b"{}")
            mensajes = ejecutar_comando_web(datos)
            self._responder(200, "application/json", json.dumps({"mensajes": mensajes}))
        except Exception as e:
            self._responder(400, "application/json", json.dumps({"mensajes": [f"[ADMIN ERROR] {e}"]}))


# Servidor web. ThreadingHTTPServer atiende cada petición en un hilo distinto
def iniciar_panel_web(puerto):
    servidor = http.server.ThreadingHTTPServer(("0.0.0.0", puerto), ManejadorPanelWeb)
    servidor.daemon_threads = True
    proteccion = "con contraseña" if os.environ.get("WM_PANEL_PASSWORD") else "SIN contraseña"
    print(f"[CENTRAL-WEB] Panel web en http://0.0.0.0:{puerto} ({proteccion})")
    servidor.serve_forever()


# ============================================================
#  PROGRAMA PRINCIPAL
# ============================================================
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python3 WM_Central.py <puerto_sockets> <ip_kafka:puerto> [puerto_web]")
        print("Ejemplo: python3 WM_Central.py 8000 localhost:9092 8080")
        sys.exit(1)

    socket_port = int(sys.argv[1])
    kafka_server = sys.argv[2]
    # Puerto web: el tercer argumento, o la variable WM_WEB_PORT, o 8080
    web_port = int(sys.argv[3]) if len(sys.argv) > 3 else int(os.environ.get("WM_WEB_PORT", 8080))

    # A partir de aquí todo lo que se imprima llega también al panel web
    sys.stdout = SalidaConRegistro(sys.stdout)

    print("=" * 50)
    print("      INICIANDO SISTEMA WM_CENTRAL")
    print("=" * 50)

    # Base de datos: todas las estaciones empiezan DESCONECTADAS
    init_db()
    print("\n--- Estado inicial de estaciones (BD) ---")
    for est in get_estaciones():
        print(f"[{est[2]}] ID: {est[0]} | Zona: {est[1]}")
    print("-" * 40)

    # Kafka y sus hilos (riegos, timeouts y latido)
    iniciar_kafka(kafka_server)

    # Servidor de sockets para los monitores
    threading.Thread(target=start_socket_server, args=(socket_port,), daemon=True).start()

    # Panel web
    threading.Thread(target=iniciar_panel_web, args=(web_port,), daemon=True).start()

    # Si hay terminal (en local) abrimos el panel de consola. En Railway no hay
    # terminal: entonces Central se queda funcionando solo como servidor
    try:
        if sys.stdin and sys.stdin.isatty():
            ejecutar_consola_admin()
        else:
            raise io.UnsupportedOperation("Entorno headless sin consola interactiva")
    except Exception as e:
        print(f"[CENTRAL] Consola interactiva omitida ({e}). Modo servidor continuo activo.")
        print("[CENTRAL] A la escucha de sockets TCP y Kafka...")
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            print("[CENTRAL] Servidor detenido.")
