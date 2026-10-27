#funciónn principal:
# - estar siempre encendido a la escucha
# - aceptar conexiones simultáneas de múltiples estaciones de riego (WM_WS_M)
# - autenticarlas usando la base de datos SQLite
# - Reaccionar de inmediato si alguna se avería
# - (Kafka) recibir las peticiones de riego de los operarios, validar que la estación
#esté disponible, ordenar la apertura al Engine, responder al operario y enviarle
#el resumen final del riego
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
logging.getLogger("kafka").addHandler(logging.NullHandler())

ENQ = b'\x05'   # Solicitud inicial
ACK = b'\x06'   # Confirmación OK
NACK = b'\x15'  # Error / No reconocido

# Códigos de escape ANSI para formato visual y colores del panel
RESET = "\033[0m"
BOLD = "\033[1m"

COLOR_VERDE = "\033[32m"
COLOR_VERDE_PARPADEO = "\033[5;32m"  # Parpadeo verde para REGANDO
COLOR_ROJO = "\033[31m"              # Rojo para AVERIADA / FUGA
COLOR_NARANJA = "\033[33m"           # Amarillo / Naranja para FUERA DE SERVICIO
COLOR_GRIS = "\033[90m"              # Gris para DESCONECTADA

def colorear_estado(estado: str) -> str:
    """Devuelve el estado formateado con su color ANSI correspondiente."""
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


# Registro de sockets activos hacia los Monitores {ws_id: client_socket}
lock_monitores = threading.Lock()
monitores_conectados = {}
bloqueadas_con_averia = set()


def notificar_monitor(ws_id: str, orden: str):
    """Envía una orden al monitor de una estación por socket (si está conectado)."""
    with lock_monitores:
        sock = monitores_conectados.get(ws_id)
    if sock:
        try:
            sock.sendall(orden.encode("utf-8") + b"\n")
        except Exception:
            pass

TIMEOUT_MONITOR = 6     # s sin recibir nada del Monitor (ni PING) -> DESCONECTADA
TIMEOUT_HANDSHAKE = 10  # s máximos para completar ENQ + REG; si no, se cierra la conexión


def procesar_trama_monitor(ws_id, texto):
    """Interpreta una trama recibida del Monitor ya autenticado."""
    if texto == "PING":
        return   # latido: solo sirve para saber que el Monitor sigue vivo

    if texto == "ALERT#FUGA":
        if get_estado_estacion(ws_id) == "FUERA DE SERVICIO":
            bloqueadas_con_averia.add(ws_id)   # recordar que estaba bloqueada
        actualizar_estado_estacion(ws_id, "AVERIADA")
        print(f"\n[ALERTA CRÍTICA] ¡Avería/Fuga detectada en {ws_id}! -> Estado en BD: AVERIADA\n")

    elif texto == "ALERT#RESUELTA":
        estado = get_estado_estacion(ws_id)
        if estado == "AVERIADA":
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


#cada vez que un Monitor se conecta a la central el servidor principal geenera un hilo que ejecuta la funciòn exclusiva para esa conexión
def handle_monitor_client(client_socket, client_address):
    ws_id = None
    print(f"\n[CENTRAL] Conexión TCP entrante desde {client_address}") #client address--> tupla de la IP y puerto
    try:
        # Si en TIMEOUT_HANDSHAKE s no completa el saludo y el registro, se cierra:
        # una conexión que no dice nada no puede tener un hilo ocupado para siempre.
        client_socket.settimeout(TIMEOUT_HANDSHAKE)
        #Handshake inicial
        data = client_socket.recv(1024)#hilo se pausa hasta que llegan bytes por la red
        if data != ENQ:
            print(f"[CENTRAL] Byte recibido incorrecto ({data}). Enviando <NACK>...")
            client_socket.sendall(NACK)
            return

        client_socket.sendall(ACK)
        print(f"[CENTRAL] Handshake superado con {client_address}. Esperando registro...")

        # convierte los bytes entrantes en texto legible mediante 'decode'
        reg_data = client_socket.recv(1024).decode('utf-8').strip()
        #cada vez que nos envíemos datos separaremos cada dato con una almohadilla
        partes = reg_data.split("#")

        if len(partes) == 3 and partes[0] == "REG":
            ws_id = partes[1]
            ubicacion = partes[2]

            if autenticar_estacion(ws_id, ubicacion):
                with lock_monitores:
                    duplicado = ws_id in monitores_conectados
                    if not duplicado:
                        monitores_conectados[ws_id] = client_socket  # <--- GUARDAR AQUÍ
                if duplicado:
                    # Ya hay un Monitor vivo con este ID: no dejamos que otro lo suplante.
                    # (Si el anterior estaba colgado, el latido lo da por muerto en
                    # TIMEOUT_MONITOR s y el nuevo entrará en su siguiente reintento.)
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

        #Mantener la escucha de alertas mientras la estación siga viva.
        #El Monitor manda un PING cada 2 s: si en TIMEOUT_MONITOR s no llega nada
        #(Monitor colgado, red cortada sin cierre...), la damos por DESCONECTADA.
        client_socket.settimeout(TIMEOUT_MONITOR)
        buffer = ""
        while True:
            try:
                msg = client_socket.recv(1024)
            except socket.timeout:
                print(f"\n[CENTRAL] {ws_id} no da señales de vida en {TIMEOUT_MONITOR}s. Se da por desconectada.")
                break
            if not msg:
                break

            buffer += msg.decode('utf-8', 'replace')
            # Cada trama del Monitor termina en "\n": procesamos las completas
            while "\n" in buffer:
                texto, buffer = buffer.split("\n", 1)
                procesar_trama_monitor(ws_id, texto.strip())

    except Exception as e:
        print(f"[CENTRAL] Incidencia con {ws_id or client_address}: {e}")
    finally:
        if ws_id:
            with lock_monitores:
                # Solo si ESTE socket sigue siendo el registrado: si la estación ya se ha
                # vuelto a conectar por otro socket, cerrar este no debe desconectarla.
                es_el_actual = monitores_conectados.get(ws_id) is client_socket
                if es_el_actual:
                    monitores_conectados.pop(ws_id, None)  # <--- ELIMINAR AQUÍ
            if es_el_actual:
                actualizar_estado_estacion(ws_id, "DESCONECTADA")
                print(f"[CENTRAL] Estación {ws_id} desconectada -> Estado: DESCONECTADA\n")
        client_socket.close()


#actúa como recepcionista de la central, su propósito es poner en marcha el servidor de red, quedarse esperando llamadas entrantes de los monitores
#y derivar cada conexión a un hilo independiente
def start_socket_server(port):
    #crea el objeto de socket de red
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #activa la reutilización automática del puerto local.
    #aunque hagas control + C y se vuelva a ejectuar inmediatamente
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    #enlaza el socket a una interfaz física y a un puerto. Al poner 0.0.0.0 le ordena que escuche en todas las tarjetas e interfaces de red del equipo
    server.bind(("0.0.0.0", port))
    #pone el socket en modo de escucha pasiva
    server.listen(50)   # cola de conexiones pendientes (con 5, en ráfagas se rechazaban)
    print(f"[CENTRAL] Servidor de Sockets a la escucha en el puerto {port}...")

    while True:
        client_sock, addr = server.accept()
        #cre un nuevo hilo de ejecución en paralelo
        thread = threading.Thread(target=handle_monitor_client, args=(client_sock, addr))
        #hilo en segundo plano(demonio), todos los hilos creados se destruyen al instante  sin dejar procesos huérfanos
        thread.daemon = True
        #arranca el hilo
        thread.start()



# COORDINACIÓN DE RIEGO POR KAFKA

# Topics (deben llamarse igual en WM_FO y WM_WS_E)
TOPIC_SOLICITUDES = "wm_solicitudes"   # Operario/estación -> CENTRAL: SOLICITUD, LISTAR
TOPIC_RESPUESTAS = "wm_respuestas"     # CENTRAL -> Operario: INFO, AUTORIZADO, DENEGADO, RESUMEN, LISTADO
TOPIC_ORDENES = "wm_ordenes"           # CENTRAL -> Engine: ABRIR, CERRAR
TOPIC_TELEMETRIA = "wm_telemetria"     # Engine -> CENTRAL/Operario: INICIO, TELEMETRIA, FIN, RECHAZO

TIMEOUT_APERTURA = 5    # s que esperamos a que el Engine confirme la apertura (INICIO)
TIMEOUT_SIN_NOTICIAS = 8  # s sin telemetría de un riego en curso -> se da por terminado

producer = None
lock_riegos = threading.Lock()
#Riegos pedidos al Engine pero aún sin confirmar:  ws_id -> datos de la petición
pendientes = {}
#Riegos en curso:  ws_id -> {req_id, op_id, caudal, volumen, transcurrido, sol}
activos = {}


def enviar(topic, mensaje, clave):
    mensaje["ts"] = time.time()
    producer.send(topic, key=clave, value=mensaje)
    producer.flush(timeout=5)


def responder_operario(tipo, sol, mensaje, **extra):
    """Manda un mensaje al operario que hizo la petición 'sol'."""
    datos = {"tipo": tipo, "op_id": sol.get("op_id"), "req_id": sol.get("req_id"),
             "ws_id": sol.get("ws_id"), "mensaje": mensaje}
    datos.update(extra)
    enviar(TOPIC_RESPUESTAS, datos, clave=sol.get("op_id"))


def denegar(sol, motivo):
    print(f"[CENTRAL-RIEGO] DENEGADO {sol.get('op_id')} -> {sol.get('ws_id')}: {motivo}")
    responder_operario("DENEGADO", sol, motivo)


#1. Peticiones que llegan de los operarios
def procesar_solicitud(sol):
    op_id = sol.get("op_id")
    ws_id = sol.get("ws_id")
    print(f"[CENTRAL-RIEGO] Petición de {op_id}: regar {ws_id} durante {sol.get('duracion')}s")
    responder_operario("INFO", sol, f"CENTRAL ha recibido la petición. Comprobando {ws_id}...")

    #El operario tiene que estar registrado
    interno = op_id == "CENTRAL" or str(op_id).startswith("MANUAL-")
    if not interno and not existe_operario(op_id):
        return denegar(sol, f"El operario {op_id} no está registrado en CENTRAL")

    #La estación tiene que existir, estar DISPONIBLE y no tener otro riego en marcha
    estado = get_estado_estacion(ws_id)
    if estado is None:
        return denegar(sol, f"La estación {ws_id} no existe")
    with lock_riegos:
        if ws_id in pendientes or ws_id in activos:
            return denegar(sol, f"La estación {ws_id} ya está ocupada con otro riego")
        if estado != "DISPONIBLE":
            return denegar(sol, f"La estación {ws_id} no está disponible (estado: {estado})")
        #La reservamos y le ordenamos abrir
        pendientes[ws_id] = {"enviado": time.time(), "sol": sol}

    responder_operario("INFO", sol, f"{ws_id} disponible. Solicitando apertura de la electroválvula...")
    enviar(TOPIC_ORDENES, {"tipo": "ABRIR", "ws_id": ws_id, "op_id": op_id,
                           "req_id": sol.get("req_id"),
                           "duracion": int(sol.get("duracion", 10))}, clave=ws_id)


def procesar_listado(sol):
    """El operario quiere ver las estaciones y su estado."""
    estaciones = [{"ws_id": e[0], "ubicacion": e[1], "estado": e[2]} for e in get_estaciones()]
    responder_operario("LISTADO", sol, "Estado de las estaciones", estaciones=estaciones)


#Eventos que llegan de los Engines
def procesar_evento_ws(ev):
    tipo = ev.get("tipo")
    ws_id = ev.get("ws_id")

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

    elif tipo == "RECHAZO":
        with lock_riegos:
            pend = pendientes.pop(ws_id, None)
        if pend and pend["sol"].get("req_id") == ev.get("req_id"):
            denegar(pend["sol"], f"{ws_id} rechazó la apertura: {ev.get('motivo')}")

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

    elif tipo == "FIN":
        with lock_riegos:
            act = activos.get(ws_id)
            if act and act["req_id"] != ev.get("req_id"):
                return   # FIN atrasado de un riego anterior
            activos.pop(ws_id, None)
            pendientes.pop(ws_id, None)

        motivo = ev.get("motivo")
        estado_actual = get_estado_estacion(ws_id)

        # Solo cambiamos el estado si es un riego que CENTRAL tenía en curso. Un FIN de
        # un riego que no conocemos (p.ej. terminó con CENTRAL caída) solo genera el
        # resumen para el operario: el estado real de la estación ya lo da su Monitor.
        if not act:
            pass

        # 1. Si el motivo fue una fuga detectada durante el riego
        elif motivo == "FUGA":
            actualizar_estado_estacion(ws_id, "AVERIADA")

        # 2. Si el motivo fue el bloqueo del Administrador o ya estaba marcada como FUERA DE SERVICIO
        elif motivo == "BLOQUEO_ADMIN_CENTRAL" or estado_actual in ("FUERA DE SERVICIO", "BLOQUEADA"):
            actualizar_estado_estacion(ws_id, "FUERA DE SERVICIO")

        elif motivo=="MONITOR_CAIDO":
                    actualizar_estado_estacion(ws_id, "DESCONECTADA") 

        # 3. Solo si fue un fin normal (por tiempo límite o parada voluntaria) vuelve a DISPONIBLE
        elif estado_actual == "REGANDO":
            actualizar_estado_estacion(ws_id, "DISPONIBLE")
          

        print(f"[CENTRAL-RIEGO] {ws_id} FIN de riego ({motivo}): "
              f"{ev.get('duracion')}s, {ev.get('litros')} l")
        sol = act["sol"] if act else {"op_id": ev.get("op_id"), "req_id": ev.get("req_id"),
                                      "ws_id": ws_id}
        responder_operario("RESUMEN", sol, f"Riego finalizado en {ws_id}. Motivo: {motivo}",
                           motivo=motivo, duracion=ev.get("duracion"), litros=ev.get("litros"))


def vigilar_timeouts():
    """Vigila dos cosas cada segundo:
    - Si un Engine no confirma la apertura en TIMEOUT_APERTURA s -> se deniega y se libera.
    - Si de un riego en curso no llega nada en TIMEOUT_SIN_NOTICIAS s (Engine caído o FIN
      perdido) -> se da por terminado. Si no, la estación quedaría "ocupada" para siempre."""
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
            # Si Kafka está caído, enviar() lanza excepción: sin este try el hilo
            # moriría y Central dejaría de vigilar los timeouts para siempre.
            print(f"[CENTRAL-RIEGO] Error vigilando timeouts (se reintenta): {e}")


def saltar_mensajes_antiguos(consumer):
    """Al arrancar, CENTRAL ignora lo que se quedó en Kafka mientras estaba caída
    (peticiones viejas que los operarios ya habrán dado por perdidas).
    Antes se hacía comparando el campo 'ts' del mensaje con la hora de CENTRAL,
    pero 'ts' lo pone el reloj del ordenador que envía: si ese reloj va desfasado
    (p.ej. un PC del laboratorio con 1 hora de retraso), CENTRAL descartaba TODAS
    sus peticiones. Saltando al final de los topics no dependemos de ningún reloj."""
    # OJO: solo se saltan las PETICIONES. La telemetría (INICIO/FIN de riegos) se
    # sigue leyendo desde donde se quedó: si un riego terminó con CENTRAL caída, su
    # FIN está ahí y hay que procesarlo para mandar el resumen al operario
    # (la guía exige que no se pierda ninguna transacción).
    telemetria_previa = []
    limite = time.time() + 30
    while not consumer.assignment() and time.time() < limite:
        lotes = consumer.poll(timeout_ms=200)
        for registros in lotes.values():
            telemetria_previa += [r for r in registros if r.topic == TOPIC_TELEMETRIA]
    solicitudes = [tp for tp in consumer.assignment() if tp.topic == TOPIC_SOLICITUDES]
    if solicitudes:
        consumer.seek_to_end(*solicitudes)
        for tp in solicitudes:
            consumer.position(tp)           # fija ya la posición "final"
    return telemetria_previa


#Bucle consumidor
def bucle_kafka(broker):
    # Un único consumidor para peticiones y telemetría: todas las decisiones se toman
    # en un solo hilo y en orden, así dos peticiones a la misma estación no se cruzan.
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
        except Exception as e:   # el broker se ha caído o no responde: reintentar
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
                except Exception as e:   # un mensaje malformado no debe tumbar CENTRAL
                    print(f"[CENTRAL-RIEGO] Error procesando {m}: {e}")


def crear_topics(broker):
    """Crea los topics si no existen (si falla no pasa nada: Kafka los crea solo)."""
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

def latido():
    while True:
        try:
            producer.send(TOPIC_RESPUESTAS, key="CENTRAL", value={"tipo": "LATIDO", "ts": time.time()})
        except Exception:
            pass
        time.sleep(2)

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

# =========================================================================
# FUNCIONES DE ADMINISTRACIÓN CENTRAL (Punto 11)
# =========================================================================

def admin_forzar_riego(ws_id: str, duracion: int = 20, publicar_mensaje=None):
    """Opción 'Iniciar un riego' de CENTRAL (Punto 11): se comporta igual que un operario."""
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
            "op_id": "CENTRAL",   # <--- Reconocido como interno por procesar_solicitud
            "ws_id": target,
            "duracion": duracion
        }
        enviar(TOPIC_SOLICITUDES, sol, clave="CENTRAL")
        mensaje = f"[ADMIN] Orden de riego enviada para {target} ({duracion}s)"
        if publicar_mensaje:
            publicar_mensaje(mensaje)
        else:
            print(mensaje)


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
            bloqueadas_con_averia.add(target)   # al resolverse la fuga quedará en NARANJA
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
    """Opción 'Activar la WS' (Punto 11): vuelve a estado ACTIVADA/DISPONIBLE (verde)."""
    estaciones = get_estaciones()
    destinos = [e[0] for e in estaciones] if ws_id.lower() == "all" else [ws_id]

    for target in destinos:
        bloqueadas_con_averia.discard(target)
        # Volvemos a DISPONIBLE en BD
        actualizar_estado_estacion(target, "DISPONIBLE")

        # Notificamos al monitor por socket
        notificar_monitor(target, "ORDEN#ACTIVAR")
        print(f"[ADMIN] Estación {target} ACTIVADA -> Estado: DISPONIBLE")


def ordenar_cierre(ws_id, motivo="ORDEN_CENTRAL"):
    """Corta el riego en curso de una estación (p.ej. al bloquearla)."""
    enviar(TOPIC_ORDENES, {"tipo": "CERRAR", "ws_id": ws_id, "motivo": motivo}, clave=ws_id)


def obtener_telemetria():
    """Riegos en curso {ws_id: {caudal, volumen, op_id, ...}} para pintarlos en el panel."""
    with lock_riegos:
        return {ws: {k: v for k, v in d.items() if k not in ("sol", "ultimo")}
                for ws, d in activos.items()}


# =========================================================================
# PANEL DE CONTROL Y CONSOLA DE ADMINISTRACIÓN
# =========================================================================

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


ID_VALIDO = re.compile(r"^[A-Z0-9_-]{1,20}$")


def admin_alta_estacion(ws_id: str, ubicacion: str) -> str:
    """Da de alta (o actualiza) una estación para que su Monitor pueda autenticarse.
    La ubicación tiene que coincidir EXACTAMENTE con la que se pase al Monitor."""
    ws_id = ws_id.strip().upper()
    ubicacion = ubicacion.strip().strip('"').strip("'").strip()
    if not ID_VALIDO.match(ws_id) or ws_id == "ALL":
        return "[ADMIN] ID de estación no válido (letras, números, - o _; máx. 20)."
    if not ubicacion or len(ubicacion) > 50 or "#" in ubicacion:
        return "[ADMIN] Ubicación no válida (1-50 caracteres y sin '#')."
    resultado = alta_estacion(ws_id, ubicacion)
    return f"[ADMIN] Estación {ws_id} ({ubicacion}) {resultado}. Arranca su Monitor con esa misma ubicación."


def admin_alta_operario(op_id: str, nombre: str) -> str:
    """Da de alta (o actualiza) un operario para que Central acepte sus peticiones."""
    op_id = op_id.strip().upper()
    nombre = nombre.strip().strip('"').strip("'").strip()
    if not ID_VALIDO.match(op_id) or op_id == "CENTRAL" or op_id.startswith("MANUAL-"):
        return "[ADMIN] ID de operario no válido (letras, números, - o _; máx. 20)."
    if not nombre or len(nombre) > 50:
        return "[ADMIN] Nombre no válido (1-50 caracteres)."
    resultado = alta_operario(op_id, nombre)
    return f"[ADMIN] Operario {op_id} ({nombre}) {resultado}."


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


def ejecutar_consola_admin():
    # La consola debe dibujar directamente en la terminal real (no a través del
    # registro de mensajes del panel web, o los dibujos acabarían en la web)
    terminal_real = sys.stdout.destino if isinstance(sys.stdout, SalidaConRegistro) else sys.stdout
    sys.stdout = terminal_real
    entrada = TextArea(height=1, prompt="> ", multiline=False)
    atajos = KeyBindings()
    mensaje_admin = [" "]
    lock_mensaje = threading.Lock()
    app_ref = [None]

    def publicar_mensaje(mensaje):
        with lock_mensaje:
            mensaje_admin[0] = mensaje
        if app_ref[0] is not None:
            app_ref[0].invalidate()

    def leer_mensaje():
        with lock_mensaje:
            return mensaje_admin[0]

    @atajos.add("c-c")
    def salir(event):
        event.app.exit()

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
            # patch_stdout cambia sys.stdout: lo envolvemos para que el panel web siga
            # recibiendo los mensajes mientras la consola está abierta
            sys.stdout = SalidaConRegistro(sys.stdout)
            app.run()
    finally:
        sys.stdout = SalidaConRegistro(terminal_real)


# =========================================================================
# PANEL WEB (para controlar CENTRAL cuando corre en Railway sin terminal)
# =========================================================================
# CENTRAL sirve una página web con el estado de las estaciones en tiempo real y
# botones para regar, bloquear y activar. La página pregunta el estado cada
# segundo a /api/estado y manda las órdenes a /api/comando, que reutiliza
# exactamente las mismas funciones que la consola (ejecutar_comando_admin).
#
# Seguridad: si existe la variable de entorno WM_PANEL_PASSWORD, el navegador
# pedirá usuario y contraseña (cualquier usuario + esa contraseña).

# Últimos mensajes de la aplicación, para mostrarlos en el panel web
lock_log = threading.Lock()
log_mensajes = collections.deque(maxlen=60)


class SalidaConRegistro:
    """Sustituye a sys.stdout: cada línea que imprime cualquier hilo se sigue
    escribiendo donde iba (terminal / logs de Railway) y además se guarda en
    log_mensajes para que el panel web la muestre."""
    def __init__(self, destino):
        self.destino = destino
        self.pendiente = ""

    def write(self, texto):
        try:
            self.destino.write(texto)
        except Exception:
            pass
        self.pendiente += texto
        while "\n" in self.pendiente:
            linea, self.pendiente = self.pendiente.split("\n", 1)
            linea = linea.strip()
            if linea and not linea.startswith(("=====", "-----")):
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

    def __getattr__(self, nombre):          # encoding, fileno... los da el destino real
        return getattr(self.destino, nombre)


def estado_para_web():
    """Todo lo que necesita la página: estaciones con su telemetría y mensajes."""
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


def ejecutar_comando_web(datos):
    """Traduce el botón pulsado a un comando de la consola y devuelve sus mensajes."""
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


class ManejadorPanelWeb(http.server.BaseHTTPRequestHandler):
    def log_message(self, formato, *args):
        pass   # sin esto, cada petición (una por segundo) llenaría los logs

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
            longitud = min(int(self.headers.get("Content-Length", 0)), 10_000)
            datos = json.loads(self.rfile.read(longitud) or b"{}")
            mensajes = ejecutar_comando_web(datos)
            self._responder(200, "application/json", json.dumps({"mensajes": mensajes}))
        except Exception as e:
            self._responder(400, "application/json", json.dumps({"mensajes": [f"[ADMIN ERROR] {e}"]}))


def iniciar_panel_web(puerto):
    servidor = http.server.ThreadingHTTPServer(("0.0.0.0", puerto), ManejadorPanelWeb)
    servidor.daemon_threads = True
    proteccion = "con contraseña" if os.environ.get("WM_PANEL_PASSWORD") else "SIN contraseña"
    print(f"[CENTRAL-WEB] Panel web en http://0.0.0.0:{puerto} ({proteccion})")
    servidor.serve_forever()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python3 WM_Central.py <puerto_sockets> <ip_kafka:puerto> [puerto_web]")
        print("Ejemplo: python3 WM_Central.py 8000 localhost:9092 8080")
        sys.exit(1)

    socket_port = int(sys.argv[1])
    kafka_server = sys.argv[2]
    # Puerto del panel web: 3er argumento, o variable WM_WEB_PORT, o 8080 por defecto
    web_port = int(sys.argv[3]) if len(sys.argv) > 3 else int(os.environ.get("WM_WEB_PORT", 8080))

    # Todo lo que se imprima a partir de aquí también llega al panel web
    sys.stdout = SalidaConRegistro(sys.stdout)

    print("=" * 50)
    print("      INICIANDO SISTEMA WM_CENTRAL")
    print("=" * 50)

    init_db()
    print("\n--- Estado inicial de estaciones (BD) ---")
    for est in get_estaciones():
        print(f"[{est[2]}] ID: {est[0]} | Zona: {est[1]}")
    print("-" * 40)

    iniciar_kafka(kafka_server)

    # 1. Servidor de sockets (hilo daemon)
    threading.Thread(target=start_socket_server, args=(socket_port,), daemon=True).start()

    # 2. Panel web (hilo daemon): estado en tiempo real + botones regar/bloquear/activar
    threading.Thread(target=iniciar_panel_web, args=(web_port,), daemon=True).start()

    # En Mac con terminal abre el panel; en Railway (Docker) mantiene el servidor sin TTY
    # Modo servidor cloud automático si no hay TTY o si prompt_toolkit falla al registrar el teclado
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