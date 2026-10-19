# WM_FO.py - Aplicación del operario de campo (Field Operator)
# - Modo interactivo: menú por consola para pedir riegos y ver estaciones
# - Modo fichero: lee las peticiones de un .txt, las lanza de una en una y, al
#   concluir cada servicio (éxito o fracaso), espera 4 segundos antes de la siguiente
#
# Uso:
#   py WM_FO.py --broker localhost:9092 --id FO-01
#   py WM_FO.py --broker localhost:9092 --id FO-01 --fichero solicitudes.txt
#
# Formato del fichero (una petición por línea; las líneas vacías o que empiezan por # se ignoran):
#   <ID_ESTACION>,<DURACION_EN_SEGUNDOS>
#   WS-01,15
import sys
import json
import time
import uuid
import queue
import argparse
import threading

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

TOPIC_SOLICITUDES = "wm_solicitudes"   # FO -> CENTRAL (escribe)
TOPIC_RESPUESTAS = "wm_respuestas"     # CENTRAL -> FO (lee)
TOPIC_TELEMETRIA = "wm_telemetria"     # WS_E -> CENTRAL/FO (lee)

ESPERA_ENTRE_SERVICIOS = 4      # s (lo exige el enunciado en el modo fichero)
TIMEOUT_RESPUESTA = 15          # s sin noticias de CENTRAL antes de autorizar -> fracaso (resiliencia)
TIMEOUT_TELEMETRIA = 10         # s sin telemetría durante un riego -> fracaso (resiliencia)

op_id = None #quien soy? se pone al arrancar
producer = None #el productor Kafka que se crea al arrancar
bandeja = queue.Queue()         # mensajes relevantes para este operario, queue puede tener los 2 hilos (consumidor y principal) accediendo a la vez
riego_activo = {"req_id": None, "ws_id": None}   # para filtrar la telemetría



#KAFKA
def crear_consumidor(broker):
    """Cada operario usa un group_id ÚNICO: así recibe todos los mensajes del topic
    (si dos FO compartieran grupo, Kafka repartiría los mensajes entre ellos)."""
    consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        group_id=f"wm_fo_{op_id}", #grupo único por operario 
        auto_offset_reset="latest", #empieza a leer desde ahora
        enable_auto_commit=False,      #Kafka se encarga de recordar el offset
        metadata_max_age_ms=3000,       #pregunta a kafka cada 3 segundos
        value_deserializer=lambda b: json.loads(b.decode("utf-8")), #de bytes a texto
    )
    consumer.subscribe([TOPIC_RESPUESTAS, TOPIC_TELEMETRIA]) #me apunto a los topics
    # Esperamos a tener particiones asignadas ANTES de enviar nada; si no, la respuesta
    # de CENTRAL podría llegar antes de que estemos escuchando y la perderíamos.
    limite = time.time() + 30
    while not consumer.assignment() and time.time() < limite:
        lotes = consumer.poll(timeout_ms=200)
        procesar_lotes(consumer, lotes)
    for tp in consumer.assignment():
        consumer.position(tp)       # fija el offset "latest" ya
    return consumer


def hilo_consumidor(consumer):
    while True:
        lotes = consumer.poll(timeout_ms=500)
        procesar_lotes(consumer, lotes)


def procesar_lotes(consumer, lotes):
    for registros in lotes.values():
        for r in registros:
            m = r.value
            if r.topic == TOPIC_RESPUESTAS and m.get("op_id") == op_id:
                if m.get("req_id") == riego_activo["req_id"]:
                    bandeja.put(m)
                else:
                    mostrar_pendiente(m)
            elif (r.topic == TOPIC_TELEMETRIA and m.get("tipo") == "TELEMETRIA"
                  and m.get("req_id") == riego_activo["req_id"]):
                bandeja.put(m)
    if lotes:
        try:
            consumer.commit()
        except Exception as e:
            print(f"[FO {op_id}] No se pudo guardar la posición en Kafka: {e}")
                

def mostrar_pendiente(m):
    """Muestra un mensaje que no pertenece a la petición en curso"""
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


def enviar(mensaje):
    mensaje["ts"] = time.time()
    producer.send(TOPIC_SOLICITUDES, key=op_id, value=mensaje)
    producer.flush(timeout=5)


def vaciar_bandeja():
    while not bandeja.empty():
        bandeja.get_nowait()



#Servicios
def solicitar_riego(ws_id, duracion):
    """Pide un riego y se queda acompañándolo hasta el final.
    Devuelve True si el servicio terminó con resumen, False si fue denegado o falló."""
    req_id = f"{op_id}-{uuid.uuid4().hex[:8]}"
    vaciar_bandeja()
    riego_activo.update(req_id=req_id, ws_id=ws_id)
    print(f"\n[FO {op_id}] Solicitando riego en {ws_id} durante {duracion}s (petición {req_id})")
    enviar({"tipo": "SOLICITUD", "req_id": req_id, "op_id": op_id,
            "ws_id": ws_id, "duracion": duracion})

    autorizado = False
    try:
        while True:
            espera = TIMEOUT_TELEMETRIA if autorizado else TIMEOUT_RESPUESTA
            try:
                m = bandeja.get(timeout=espera)
            except queue.Empty:
                if autorizado:
                    print(f"\n[FO {op_id}] ✖ Se ha perdido la comunicación con {ws_id} durante el riego.")
                else:
                    print(f"[FO {op_id}] ✖ CENTRAL no responde. Petición abandonada.")
                return False
            if m.get("req_id") != req_id:
                continue                      # restos de peticiones anteriores
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
                # Fuga o pérdida de la estación = servicio fallido
                return m.get("motivo") not in ("FUGA", "SIN_NOTICIAS_ESTACION", "MONITOR_CAIDO")
    finally:
        riego_activo.update(req_id=None, ws_id=None)


def listar_estaciones():
    req_id = f"{op_id}-{uuid.uuid4().hex[:8]}"
    vaciar_bandeja()
    riego_activo.update(req_id=req_id, ws_id=None)


    try:
        enviar({"tipo": "LISTAR", "req_id": req_id, "op_id": op_id})
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
        print(f"[FO {op_id}] X CENTRAL no ha respondido al listado.")
    finally:
        riego_activo.update(req_id=None, ws_id=None)


def leer_fichero(ruta):
    """Devuelve una lista de (ws_id, duracion) a partir del fichero de peticiones."""
    peticiones = []
    with open(ruta, encoding="utf-8") as f:
        for n, linea in enumerate(f, 1):
            linea = linea.strip()
            if not linea or linea.startswith("#"):
                continue
            try:
                ws_id, duracion = [p.strip() for p in linea.split(",")]
                peticiones.append((ws_id, int(duracion)))
            except ValueError:
                print(f"[FO {op_id}] Línea {n} ignorada (formato esperado WS-01,15): {linea!r}")
    return peticiones


def modo_fichero(ruta):
    peticiones = leer_fichero(ruta)
    print(f"[FO {op_id}] {len(peticiones)} peticiones cargadas de {ruta}")
    for i, (ws_id, duracion) in enumerate(peticiones, 1):
        print(f"\n[FO {op_id}] ---- Servicio {i}/{len(peticiones)} ----")
        ok = solicitar_riego(ws_id, duracion)
        print(f"[FO {op_id}] Servicio {i} {'COMPLETADO' if ok else 'FALLIDO'}.")
        if i < len(peticiones):
            print(f"[FO {op_id}] Esperando {ESPERA_ENTRE_SERVICIOS}s antes del siguiente...")
            time.sleep(ESPERA_ENTRE_SERVICIOS)
    print(f"\n[FO {op_id}] Fichero terminado.")


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

def iniciar_operario():
    global op_id, producer #para modificar las variables globales, sino crearia nuevas que solo irian a la funcion
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
            bootstrap_servers=[args.broker], #dirección de kafka
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        consumer = crear_consumidor(args.broker)
    except NoBrokersAvailable:
        print(f"✖ No hay broker Kafka disponible en {args.broker}")
        sys.exit(1)
    threading.Thread(target=hilo_consumidor, args=(consumer,), daemon=True).start()
    time.sleep(2)

    try:
        if args.fichero:
            modo_fichero(args.fichero)
        else:
            modo_interactivo()
    except KeyboardInterrupt: #esto es el cntl+c
        pass
    print(f"\n[FO {op_id}] Saliendo.")


if __name__ == "__main__":
    iniciar_operario()
