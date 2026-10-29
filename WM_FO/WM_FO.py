# ============================================================
#  WM_FO.py - Aplicación del operario de campo (Field Operator)
#
#  El operario habla con Central solo por Kafka:
#   - escribe sus peticiones (SOLICITUD, LISTAR) en wm_solicitudes
#   - lee las respuestas de Central en wm_respuestas
#   - lee la telemetría de su riego en wm_telemetria
#
#  Tiene dos modos:
#   - Interactivo: menú para pedir riegos y ver las estaciones
#   - Fichero: lee las peticiones de un .txt y las lanza de una en una,
#     esperando 4 s entre una y otra (lo pide el enunciado)
#
#  Uso:
#    py WM_FO.py --broker localhost:9092 --id FO-01
#    py WM_FO.py --broker localhost:9092 --id FO-01 --fichero solicitudes.txt
#
#  Formato del fichero (una petición por línea, las vacías y las que
#  empiezan por # se ignoran):
#    <ID_ESTACION>,<DURACION_EN_SEGUNDOS>
#    WS-01,15
# ============================================================
import sys
import json
import time
import uuid
import queue
import argparse
import threading

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import logging
logging.getLogger("kafka").addHandler(logging.NullHandler())   # para que kafka-python no llene la pantalla de avisos

TOPIC_SOLICITUDES = "wm_solicitudes"   # FO -> Central
TOPIC_RESPUESTAS = "wm_respuestas"     # Central -> FO
TOPIC_TELEMETRIA = "wm_telemetria"     # Engine -> Central y FO

ESPERA_ENTRE_SERVICIOS = 4      # segundos entre peticiones en el modo fichero
TIMEOUT_RESPUESTA = 15          # si Central no contesta en 15 s, damos la petición por perdida
TIMEOUT_TELEMETRIA = 10         # si durante un riego no llega telemetría en 10 s, lo damos por perdido

op_id = None        # nuestro ID (FO-01...), se pone al arrancar
producer = None     # productor de Kafka, se crea al arrancar

# Cola donde el hilo consumidor deja los mensajes de la petición en curso
# y el hilo principal los va recogiendo. queue.Queue ya está preparada para
# usarse desde dos hilos a la vez
bandeja = queue.Queue()

riego_activo = {"req_id": None, "ws_id": None}   # petición en curso, para quedarnos solo con sus mensajes
ultimo_latido = time.time()                        # última vez que recibimos el LATIDO de Central


# ------------------------------------------------------------
#  Kafka
# ------------------------------------------------------------

# Crea el consumidor. Cada operario tiene su propio grupo (wm_fo_FO-01...):
# si dos operarios compartieran grupo, Kafka les repartiría los mensajes y
# alguno se quedaría sin su respuesta. Además, como el grupo es siempre el
# mismo, Kafka recuerda por dónde íbamos y al volver vemos lo pendiente
def crear_consumidor(broker):
    consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        group_id=f"wm_fo_{op_id}",
        auto_offset_reset="latest",     # la primera vez empezamos a leer desde ahora
        enable_auto_commit=False,       # guardamos la posición nosotros (en procesar_lotes)
        metadata_max_age_ms=3000,       # refresca la información de Kafka cada 3 s
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),   # de bytes JSON a diccionario
    )
    consumer.subscribe([TOPIC_RESPUESTAS, TOPIC_TELEMETRIA])
    # Esperamos a que Kafka nos asigne los topics ANTES de mandar nada.
    # Si no, la respuesta de Central podría llegar antes de estar escuchando
    limite = time.time() + 30
    while not consumer.assignment() and time.time() < limite:
        lotes = consumer.poll(timeout_ms=200)
        procesar_lotes(consumer, lotes)
    for tp in consumer.assignment():
        consumer.position(tp)
    return consumer


# Hilo que lee de Kafka todo el rato
def hilo_consumidor(consumer):
    while True:
        lotes = consumer.poll(timeout_ms=500)
        procesar_lotes(consumer, lotes)


# Reparte cada mensaje que llega:
#   LATIDO de Central             -> apuntamos que Central sigue viva
#   respuesta para nosotros       -> a la bandeja si es de la petición en curso,
#                                    si no, es de una anterior y la enseñamos aparte
#   telemetría de nuestro riego   -> a la bandeja
def procesar_lotes(consumer, lotes):
    global ultimo_latido
    for registros in lotes.values():
        for r in registros:
            m = r.value
            if m.get("tipo") == "LATIDO":
                ultimo_latido = time.time()
            elif r.topic == TOPIC_RESPUESTAS and m.get("op_id") == op_id:
                if m.get("req_id") == riego_activo["req_id"]:
                    bandeja.put(m)
                else:
                    mostrar_pendiente(m)
            elif (r.topic == TOPIC_TELEMETRIA and m.get("tipo") == "TELEMETRIA"
                  and m.get("req_id") == riego_activo["req_id"]):
                bandeja.put(m)
    # Guardamos en Kafka hasta dónde hemos leído
    if lotes:
        try:
            consumer.commit()
        except Exception:
            print(f"[FO {op_id}] Reconectando con el gestor de colas...")


# Enseña un mensaje que no es de la petición en curso. Pasa, por ejemplo,
# cuando el operario se cerró en mitad de un riego y al volver le llega el resumen
def mostrar_pendiente(m):
    if m.get("tipo") == "RESUMEN":
        print(f"\n[FO {op_id}] Resultado pendiente de un riego anterior ({m.get('req_id')}):")
        print(f"   Estación : {m.get('ws_id')}")
        print(f"   Motivo   : {m.get('motivo')}")
        print(f"   Duración : {m.get('duracion')} s")
        print(f"   Litros   : {m.get('litros')} l")
        print("   ================================")
    elif m.get("tipo") in ("AUTORIZADO", "DENEGADO"):
        print(f"\n[FO {op_id}] Mensaje pendiente de un riego anterior ({m.get('req_id')}):")
        print(f"   {m.get('tipo')} - {m.get('mensaje')}")


# Hilo que avisa si Central deja de mandar su LATIDO durante más de 6 s
def vigilar_central():
    caida = False
    while True:
        time.sleep(1)
        if time.time() - ultimo_latido > 6:
            if not caida:
                print(f"\n[FO {op_id}] ✖ Imposible conectar con WM_Central / gestor de colas. Esperando...")
                caida = True
        elif caida:
            print(f"\n[FO {op_id}] ✔ WM_Central vuelve a estar disponible.")
            caida = False


# Manda una petición a Central. Devuelve False si Kafka no responde
def enviar(mensaje):
    mensaje["ts"] = time.time()
    try:
        producer.send(TOPIC_SOLICITUDES, key=op_id, value=mensaje)
        producer.flush(timeout=5)
        return True
    except Exception:
        print(f"[FO {op_id}] ✖ Imposible conectar con WM_Central / gestor de colas.")
        return False


# Tira lo que quede en la bandeja de peticiones anteriores
def vaciar_bandeja():
    while not bandeja.empty():
        bandeja.get_nowait()


# ------------------------------------------------------------
#  Servicios
# ------------------------------------------------------------

# Pide un riego y se queda esperando hasta que termina.
# Devuelve True si acabó bien y False si se denegó o se perdió por el camino
def solicitar_riego(ws_id, duracion):
    # Cada petición lleva un id único para saber qué mensajes son suyos
    req_id = f"{op_id}-{uuid.uuid4().hex[:8]}"
    vaciar_bandeja()
    riego_activo.update(req_id=req_id, ws_id=ws_id)
    print(f"\n[FO {op_id}] Solicitando riego en {ws_id} durante {duracion}s (petición {req_id})")
    if not enviar({"tipo": "SOLICITUD", "req_id": req_id, "op_id": op_id,
                   "ws_id": ws_id, "duracion": duracion}):
        riego_activo.update(req_id=None, ws_id=None)
        return False

    autorizado = False
    try:
        while True:
            # Antes de autorizar esperamos 15 s; durante el riego, 10 s entre mensajes
            espera = TIMEOUT_TELEMETRIA if autorizado else TIMEOUT_RESPUESTA
            try:
                m = bandeja.get(timeout=espera)
            except queue.Empty:
                if autorizado:
                    print(f"\n[FO {op_id}] ✖ Se ha perdido la comunicación con {ws_id} durante el riego.")
                else:
                    print(f"[FO {op_id}] ✖ Imposible conectar con WM_Central. Petición abandonada.")
                return False
            if m.get("req_id") != req_id:
                continue
            tipo = m.get("tipo")
            if tipo == "INFO":
                print(f"[FO {op_id}] · {m['mensaje']}")
            elif tipo == "DENEGADO":
                print(f"[FO {op_id}] ✖ DENEGADO: {m['mensaje']}")
                return False
            elif tipo == "AUTORIZADO":
                autorizado = True
                print(f"[FO {op_id}] ✔ {m['mensaje']}")
            elif tipo == "TELEMETRIA":
                print(f"[FO {op_id}]   {ws_id} regando | caudal {m['caudal']:5.2f} l/min | "
                      f"acumulado {m['volumen']:7.2f} l | {m.get('transcurrido', 0):5.1f}s")
            elif tipo == "RESUMEN":
                print(f"\n[FO {op_id}] ===== RESUMEN DEL SERVICIO =====")
                print(f"   Estación : {ws_id}")
                print(f"   Motivo   : {m.get('motivo')}")
                print(f"   Duración : {m.get('duracion')} s")
                print(f"   Litros   : {m.get('litros')} l")
                print("   ================================")
                # Si terminó por fuga o porque se perdió la estación, cuenta como fallido
                return m.get("motivo") not in ("FUGA", "SIN_NOTICIAS_ESTACION", "MONITOR_CAIDO", "RECUPERADO_TRAS_CAIDA")
    finally:
        riego_activo.update(req_id=None, ws_id=None)


# Pide a Central la lista de estaciones y la enseña. Espera la respuesta 5 s
def listar_estaciones():
    req_id = f"{op_id}-{uuid.uuid4().hex[:8]}"
    vaciar_bandeja()
    riego_activo.update(req_id=req_id, ws_id=None)

    try:
        if not enviar({"tipo": "LISTAR", "req_id": req_id, "op_id": op_id}):
            return
        limite = time.time() + 5
        while time.time() < limite:
            try:
                m = bandeja.get(timeout=limite - time.time())
            except queue.Empty:
                break
            if m.get("tipo") == "LISTADO" and m.get("req_id") == req_id:
                print("\n  ID      UBICACIÓN               ESTADO")
                for e in m["estaciones"]:
                    marca = "  <- disponible" if e["estado"] == "DISPONIBLE" else ""
                    print(f"  {e['ws_id']:<7} {e['ubicacion']:<23} {e['estado']}{marca}")
                return
        print(f"[FO {op_id}] X Imposible conectar con WM_Central.")
    finally:
        riego_activo.update(req_id=None, ws_id=None)


# Lee el fichero de peticiones y devuelve [(ws_id, duracion), ...].
# Las líneas mal escritas se saltan avisando. Si el fichero no se puede
# abrir, devuelve None (y el operario sigue funcionando)
def leer_fichero(ruta):
    peticiones = []
    # Al copiar una ruta en Windows se añaden comillas: las quitamos
    ruta = ruta.strip().strip('"').strip("'")
    try:
        with open(ruta, encoding="utf-8") as f:
            for n, linea in enumerate(f, 1):
                linea = linea.strip()
                if not linea or linea.startswith("#"):
                    continue
                try:
                    ws_id, duracion = [p.strip() for p in linea.split(",")]
                    peticiones.append((ws_id.upper(), int(duracion)))
                except ValueError:
                    print(f"[FO {op_id}] Línea {n} ignorada (formato esperado WS-01,15): {linea!r}")
    except OSError as e:
        print(f"[FO {op_id}] ✖ No se puede abrir el fichero '{ruta}': {e.strerror or e}")
        return None
    return peticiones


# Modo fichero: lanza las peticiones de una en una y espera 4 s entre ellas,
# tanto si la anterior ha ido bien como si no
def modo_fichero(ruta):
    peticiones = leer_fichero(ruta)
    if peticiones is None:
        return
    # chr(34) es la comilla doble y chr(39) la simple
    print(f"[FO {op_id}] {len(peticiones)} peticiones cargadas de {ruta.strip().strip(chr(34)).strip(chr(39))}")
    for i, (ws_id, duracion) in enumerate(peticiones, 1):
        print(f"\n[FO {op_id}] ---- Servicio {i}/{len(peticiones)} ----")
        ok = solicitar_riego(ws_id, duracion)
        print(f"[FO {op_id}] Servicio {i} {'COMPLETADO' if ok else 'FALLIDO'}.")
        if i < len(peticiones):
            print(f"[FO {op_id}] Esperando {ESPERA_ENTRE_SERVICIOS}s antes del siguiente...")
            time.sleep(ESPERA_ENTRE_SERVICIOS)
    print(f"\n[FO {op_id}] Fichero terminado.")


# Menú del operario
def modo_interactivo():
    while True:
        print("\n========== MENÚ OPERARIO", op_id, "==========")
        print(" 1. Solicitar riego")
        print(" 2. Ver estaciones")
        print(" 3. Ejecutar peticiones desde fichero")
        print(" 0. Salir")
        try:
            opcion = input("Opción: ").strip()
        except EOFError:
            return
        if opcion == "1":
            ws_id = input("  ID de la estación (ej. WS-01): ").strip().upper()
            dur = input("  Duración en segundos [10]: ").strip()
            solicitar_riego(ws_id, int(dur) if dur.isdigit() else 10)
        elif opcion == "2":
            listar_estaciones()
        elif opcion == "3":
            modo_fichero(input("  Ruta del fichero: ").strip())
        elif opcion == "0":
            return


# ------------------------------------------------------------
#  Programa principal
# ------------------------------------------------------------
def iniciar_operario():
    # global: queremos cambiar las variables de arriba, no crear unas nuevas dentro de la función
    global op_id, producer, ultimo_latido
    parser = argparse.ArgumentParser(description="Aplicación del Operario (WM_FO)")
    parser.add_argument("--broker", required=True, help="IP y puerto de Kafka (ej: localhost:9092)")
    parser.add_argument("--id", required=True, help="ID único del operario (ej: FO-01)")
    parser.add_argument("--fichero", help="Fichero con peticiones (modo automático)")
    args = parser.parse_args()
    op_id = args.id

    print("=======================================")
    print("🚜 INICIANDO TERMINAL DE OPERARIO")
    print(f"👤 ID del Operario: {op_id}")
    print(f"📡 Conectando al broker: {args.broker}")
    print("=======================================\n")

    try:
        producer = KafkaProducer(
            bootstrap_servers=[args.broker],
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",          # Kafka confirma que ha guardado el mensaje
            retries=5,
            max_block_ms=5000,
        )
        consumer = crear_consumidor(args.broker)
    except NoBrokersAvailable:
        print(f"✖ No hay broker Kafka disponible en {args.broker}")
        sys.exit(1)

    # Hilos en segundo plano: uno lee de Kafka y otro vigila el latido de Central
    threading.Thread(target=hilo_consumidor, args=(consumer,), daemon=True).start()
    ultimo_latido = time.time()
    threading.Thread(target=vigilar_central, daemon=True).start()
    time.sleep(2)

    try:
        if args.fichero:
            modo_fichero(args.fichero)
        else:
            modo_interactivo()
    except KeyboardInterrupt:   # Ctrl+C
        pass
    print(f"\n[FO {op_id}] Saliendo.")


if __name__ == "__main__":
    iniciar_operario()
