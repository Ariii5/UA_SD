#funciónn principal:
# - estar siempre encendido a la escucha
# - aceptar conexiones simultáneas de múltiples estaciones de riego (WM_WS_M)
# - autenticarlas usando la base de datos SQLite
# - Reaccionar de inmediato si alguna se avería
import sys
import json
import time
import uuid
import socket
import threading
from db_manager import *

ENQ = b'\x05'   # Solicitud inicial
ACK = b'\x06'   # Confirmación OK
NACK = b'\x15'  # Error / No reconocido

#cada vez que un Monitor se conecta a la central el servidor principal geenera un hilo que ejecuta la funciòn exclusiva para esa conexión
def handle_monitor_client(client_socket, client_address):
    ws_id = None 
    print(f"\n[CENTRAL] Conexión TCP entrante desde {client_address}") #client address--> tupla de la IP y puerto
    try:
        # 1. Handshake inicial
        data = client_socket.recv(1024)#hilo se pausa hasta que llegan bytes por la red
        if data != ENQ: 
            print(f"[CENTRAL] Byte recibido incorrecto ({data}). Enviando <NACK>...")
            client_socket.sendall(NACK)
            return
        
        client_socket.sendall(ACK)
        print(f"[CENTRAL] Handshake superado con {client_address}. Esperando registro...")

        # convierte los bytes entrantes en texto legible mediante 'decode'
        reg_data = client_socket.recv(1024).decode('utf-8').strip()
        #'cada vez que nos envíemos datos separaremos cada dato con una almohadilla
        partes = reg_data.split("#")

        if len(partes) == 3 and partes[0] == "REG":
            ws_id = partes[1]
            ubicacion = partes[2]

            if autenticar_estacion(ws_id, ubicacion):
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

        # 3. Mantener la escucha de alertas mientras la estación siga viva
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
        print(f"[CENTRAL] Incidencia con {ws_id or client_address}: {e}")
    finally:
        if ws_id:
            actualizar_estado_estacion(ws_id, "DESCONECTADA")
            print(f"[CENTRAL] Estación {ws_id} desconectada -> Estado: DESCONECTADA\n")
        if ws_id:
            actualizar_estado_estacion(ws_id, "DESCONECTADA")
            print(f"[CENTRAL] Estación {ws_id} desconectada -> Estado: DESCONECTADA\n")
        client_socket.close()
    


#actúa como recepcionista de la central, su propósito es poner en marcha el servidor de red, quedarse esperando llamadas entrantes de los monitores
#de ruefi (WM_WS_M) y derivar cada conexión a un hilo independiente
def start_socket_server(port):
    #crea el objeto de socket de red
    #crea el objeto de socket de red
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #activa la reutilización automática del puerto local.
    #aunque hagas contro + C y lo vuelvas a usar inmediatamente
    #activa la reutilización automática del puerto local.
    #aunque hagas contro + C y lo vuelvas a usar inmediatamente
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    #enlaza el socket a una interfaz física y a un puerto. Al poner 0.0.0.0 le ordena que escuche en todas las tarjetas e interfaces de red del equipo

    #enlaza el socket a una interfaz física y a un puerto. Al poner 0.0.0.0 le ordena que escuche en todas las tarjetas e interfaces de red del equipo
    server.bind(("0.0.0.0", port))
    #pone el socket en modo de escucha pasiva
    #pone el socket en modo de escucha pasiva
    server.listen(5)
    print(f"[CENTRAL] Servidor de Sockets a la escucha en el puerto {port}...")

    while True:
        client_sock, addr = server.accept()
        #cre un nuevo hilo de ejecución en paralelo
        #cre un nuevo hilo de ejecución en paralelo
        thread = threading.Thread(target=handle_monitor_client, args=(client_sock, addr))
        #hilo en segundo plano(demonio), todos los hilos creados se destruyen al instante  sin dejar procesos huérfanos
        #hilo en segundo plano(demonio), todos los hilos creados se destruyen al instante  sin dejar procesos huérfanos
        thread.daemon = True
        #arranca el hilo
        #arranca el hilo
        thread.start()


#coordinación de riego con kafka: peticiones de riego, órdenes a los Engines, telemetría y resúmenes finales
TOPIC_SOLICITUDES = "wm_solicitudes"   # Operario/estación -> CENTRAL : SOLICITUD, LISTAR
TOPIC_RESPUESTAS = "wm_respuestas"     # CENTRAL -> Operario         : INFO, AUTORIZADO, DENEGADO, RESUMEN, LISTADO
TOPIC_ORDENES = "wm_ordenes"           # CENTRAL -> Engine           : ABRIR, CERRAR
TOPIC_TELEMETRIA = "wm_telemetria"     # Engine -> CENTRAL/Operario  : INICIO, TELEMETRIA, FIN, RECHAZO

TIMEOUT_APERTURA = 5    # segundos que esperamos a que el Engine confirme la apertura (INICIO)
MAX_ANTIGUEDAD = 30     # peticiones más viejas se descartan (p.ej. Central estaba caída)

producer = None
lock_riegos = threading.Lock()
# Riegos pedidos al Engine pero aún sin confirmar:  ws_id -> datos de la petición
pendientes = {}
# Riegos en curso:  ws_id -> {req_id, op_id, caudal, volumen, transcurrido, sol}
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


# 1. Peticiones que llegan de los operarios
def procesar_solicitud(sol):
    op_id = sol.get("op_id")
    ws_id = sol.get("ws_id")
    print(f"[CENTRAL-RIEGO] Petición de {op_id}: regar {ws_id} durante {sol.get('duracion')}s")
    responder_operario("INFO", sol, f"CENTRAL ha recibido la petición. Comprobando {ws_id}...")

    # a) El operario tiene que estar registrado (salvo si lo pide CENTRAL o la propia estación)
    interno = op_id == "CENTRAL" or str(op_id).startswith("MANUAL-")
    if not interno and not existe_operario(op_id):
        return denegar(sol, f"El operario {op_id} no está registrado en CENTRAL")

    # b) La estación tiene que existir, estar DISPONIBLE y no tener otro riego en marcha
    estado = get_estado_estacion(ws_id)
    if estado is None:
        return denegar(sol, f"La estación {ws_id} no existe")
    with lock_riegos:
        if ws_id in pendientes or ws_id in activos:
            return denegar(sol, f"La estación {ws_id} ya está ocupada con otro riego")
        if estado != "DISPONIBLE":
            return denegar(sol, f"La estación {ws_id} no está disponible (estado: {estado})")
        # c) La reservamos y le ordenamos abrir
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
                              "sol": pend["sol"]}
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
        print(f"[CENTRAL-RIEGO] {ws_id} | {ev.get('caudal'):5.2f} l/min | "
              f"{ev.get('volumen'):7.2f} l | operario {ev.get('op_id')}")

    elif tipo == "FIN":
        with lock_riegos:
            act = activos.pop(ws_id, None)
            pendientes.pop(ws_id, None)
        motivo = ev.get("motivo")
        # Estado final: fuga -> AVERIADA; si seguía REGANDO -> vuelve a DISPONIBLE.
        # (Si mientras tanto pasó a AVERIADA, DESCONECTADA o FUERA_SERVICIO, se respeta.)
        if motivo == "FUGA":
            actualizar_estado_estacion(ws_id, "AVERIADA")
        elif get_estado_estacion(ws_id) == "REGANDO":
            actualizar_estado_estacion(ws_id, "DISPONIBLE")
        print(f"[CENTRAL-RIEGO] {ws_id} FIN de riego ({motivo}): "
              f"{ev.get('duracion')}s, {ev.get('litros')} l")
        sol = act["sol"] if act else {"op_id": ev.get("op_id"), "req_id": ev.get("req_id"),
                                      "ws_id": ws_id}
        responder_operario("RESUMEN", sol, f"Riego finalizado en {ws_id}. Motivo: {motivo}",
                           motivo=motivo, duracion=ev.get("duracion"), litros=ev.get("litros"))


def vigilar_timeouts():
    """Si un Engine no confirma la apertura en TIMEOUT_APERTURA s, se deniega y se libera."""
    while True:
        time.sleep(1)
        ahora = time.time()
        with lock_riegos:
            caducados = [(ws, p) for ws, p in pendientes.items()
                         if ahora - p["enviado"] > TIMEOUT_APERTURA]
            for ws, _ in caducados:
                del pendientes[ws]
        for ws, p in caducados:
            denegar(p["sol"], f"{ws} no ha respondido a la orden de apertura")


#bucle consumidor
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


#Funciones para el panel y la administración
def forzar_riego(ws_id, duracion=30):
    """Opción 'Iniciar un riego' de CENTRAL: se comporta igual que si lo pidiera un operario."""
    enviar(TOPIC_SOLICITUDES, {"tipo": "SOLICITUD", "req_id": f"CEN-{uuid.uuid4().hex[:8]}",
                               "op_id": "CENTRAL", "ws_id": ws_id, "duracion": duracion},
           clave="CENTRAL")


def ordenar_cierre(ws_id, motivo="ORDEN_CENTRAL"):
    """Corta el riego en curso de una estación"""
    enviar(TOPIC_ORDENES, {"tipo": "CERRAR", "ws_id": ws_id, "motivo": motivo}, clave=ws_id)


def obtener_telemetria():
    """Riegos en curso {ws_id: {caudal, volumen, op_id, ...}} para pintarlos en el panel."""
    with lock_riegos:
        return {ws: {k: v for k, v in d.items() if k != "sol"} for ws, d in activos.items()}


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

    # Coordinación de riego por Kafka (peticiones, órdenes, telemetría y resúmenes)
    iniciar_kafka(kafka_server)

    start_socket_server(socket_port)