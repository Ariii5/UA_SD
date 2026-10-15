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
import time
import uuid
import socket
import threading
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

def notificar_monitor(ws_id: str, orden: str):
    """Envía una orden al monitor de una estación por socket (si está conectado)."""
    with lock_monitores:
        sock = monitores_conectados.get(ws_id)
    if sock:
        try:
            sock.sendall(orden.encode("utf-8") + b"\n")
        except Exception:
            pass

#cada vez que un Monitor se conecta a la central el servidor principal geenera un hilo que ejecuta la funciòn exclusiva para esa conexión
def handle_monitor_client(client_socket, client_address):
    ws_id = None
    print(f"\n[CENTRAL] Conexión TCP entrante desde {client_address}") #client address--> tupla de la IP y puerto
    try:
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
                actualizar_estado_estacion(ws_id, "DISPONIBLE")
                with lock_monitores:
                    monitores_conectados[ws_id] = client_socket  # <--- GUARDAR AQUÍ
                client_socket.sendall(ACK)
                print(f"[CENTRAL] Estación {ws_id} ({ubicacion}) autenticada con éxito -> Estado: DISPONIBLE")
            else:
                client_socket.sendall(NACK)
                print(f"[CENTRAL] Error: Estación {ws_id} no coincide en BD.")
                return
        else:
            client_socket.sendall(NACK)
            return

        #Mantener la escucha de alertas mientras la estación siga viva
        while True:
            #el hilo se queda escuchando sin gastar ciclos
            msg = client_socket.recv(1024)
            if not msg:
                break

            texto = msg.decode('utf-8').strip()
            if texto == "ALERT#FUGA":
                actualizar_estado_estacion(ws_id, "AVERIADA")
                print(f"\n[ALERTA CRÍTICA] ¡Avería/Fuga detectada en {ws_id}! -> Estado en BD: AVERIADA\n")

    except Exception as e:
        print(f"[CENTRAL] Incidencia con {ws_id or client_address}: {e}")
    finally:
        if ws_id:
            with lock_monitores:
                monitores_conectados.pop(ws_id, None)  # <--- ELIMINAR AQUÍ
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
    server.listen(5)
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
MAX_ANTIGUEDAD = 30   # s: peticiones más viejas se descartan (p.ej. Central estaba caída)

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

        # 1. Si el motivo fue una fuga detectada durante el riego
        if motivo == "FUGA":
            actualizar_estado_estacion(ws_id, "AVERIADA")

        # 2. Si el motivo fue el bloqueo del Administrador o ya estaba marcada como FUERA DE SERVICIO
        elif motivo == "BLOQUEO_ADMIN_CENTRAL" or estado_actual in ("FUERA DE SERVICIO", "BLOQUEADA"):
            actualizar_estado_estacion(ws_id, "FUERA DE SERVICIO")

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
    print(f"[CENTRAL-RIEGO] Escuchando '{TOPIC_SOLICITUDES}' y '{TOPIC_TELEMETRIA}' en {broker}")
    while True:
        lotes = consumer.poll(timeout_ms=500)
        for registros in lotes.values():
            for msg in registros:
                m = msg.value
                try:
                    if msg.topic == TOPIC_SOLICITUDES:
                        if time.time() - m.get("ts", 0) > MAX_ANTIGUEDAD:
                            continue   # petición caducada
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


def iniciar_kafka(broker):
    global producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except NoBrokersAvailable:
        print(f"[CENTRAL-RIEGO] AVISO: no hay Kafka en {broker}. Central funcionará solo con sockets.")
        return
    crear_topics(broker)
    threading.Thread(target=bucle_kafka, args=(broker,), daemon=True).start()
    threading.Thread(target=vigilar_timeouts, daemon=True).start()

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
    """Opción 'Bloquear la WS' (Punto 11): para riegos en curso y pone FUERA DE SERVICIO (naranja)."""
    estaciones = get_estaciones()
    destinos = [e[0] for e in estaciones] if ws_id.lower() == "all" else [ws_id]

    for target in destinos:
        estado = get_estado_estacion(target)
        with lock_riegos:
            riego_activo = target in activos

        if estado == "REGANDO" or riego_activo:
            ordenar_cierre(target, motivo="BLOQUEO_ADMIN_CENTRAL")

        # 2. Marcamos en BD como FUERA DE SERVICIO
        actualizar_estado_estacion(target, "FUERA DE SERVICIO")

        # 3. Notificamos al monitor por socket si está conectado
        notificar_monitor(target, "ORDEN#BLOQUEAR")
        print(f"[ADMIN] Estación {target} BLOQUEADA -> Estado: FUERA DE SERVICIO")


def admin_activar_estacion(ws_id: str):
    """Opción 'Activar la WS' (Punto 11): vuelve a estado ACTIVADA/DISPONIBLE (verde)."""
    estaciones = get_estaciones()
    destinos = [e[0] for e in estaciones] if ws_id.lower() == "all" else [ws_id]

    for target in destinos:
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
    return "\n".join(lineas)


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
            admin_bloquear_estacion(partes[1].upper())
            publicar_mensaje(f"[ADMIN] Estación {partes[1].upper()} bloqueada.")
        elif accion == "activar" and len(partes) == 2:
            admin_activar_estacion(partes[1].upper())
            publicar_mensaje(f"[ADMIN] Estación {partes[1].upper()} activada.")
        else:
            publicar_mensaje(
                "[ADMIN] Uso: regar <WS_ID|all> <segundos>, "
                "bloquear <WS_ID|all>, activar <WS_ID|all>"
            )
    except Exception as exc:
        publicar_mensaje(f"[ADMIN ERROR] {exc}")


def ejecutar_consola_admin():
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

    with patch_stdout():
        app.run()


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

    iniciar_kafka(kafka_server)

    # 1. Servidor de sockets (hilo daemon)
    threading.Thread(target=start_socket_server, args=(socket_port,), daemon=True).start()

    ejecutar_consola_admin()