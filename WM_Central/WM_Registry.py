# ============================================================
#  WM_Registry.py - Registro de estaciones (API REST)
#
#  Las estaciones se dan de alta y de baja aquí. Solo las registradas
#  pueden autenticarse después en la Central por socket.
#
#  Rutas (JSON):
#    POST   /registry/estaciones       Alta. Body {"id": "WS-05", "ubicacion": "Parque Norte"}
#                                      201 creada · 409 ya registrada · 400 datos mal
#    GET    /registry/estaciones/{id}  ¿Está registrada?   200 · 404 no existe
#    DELETE /registry/estaciones/{id}  Baja.               200 · 404 no estaba registrada
#
#  Dónde se ejecuta: dentro del proceso de la Central, en el mismo servidor web
#  que el panel (por eso todas las rutas empiezan por /registry). Motivos:
#   - En Railway un servicio solo tiene un dominio público y un TCP Proxy: no
#     se puede publicar un tercer puerto para el Registry.
#   - Usa la misma base de datos que la Central (el volumen solo se puede
#     montar en un servicio).
#   - Escribe en la misma auditoría. Si fuera otro proceso, los dos llevarían
#     su propio contador seq y se repetirían números.
#
#  Release 2, semana 2: el alta devolverá el token y la clave pública de la
#  Central, y el HTTPS lo pone Railway en su dominio.
# ============================================================
import json
import re
from db_manager import registrar_estacion, get_registro_estacion, baja_estacion
from auditoria import auditar

PREFIJO = "/registry"
RUTA_LISTA = PREFIJO + "/estaciones"
# /registry/estaciones/WS-05 -> el grupo ( ) se queda con "WS-05"
RUTA_UNA = re.compile(r"^/registry/estaciones/([^/]+)$")
# Un id válido: letras, números, guion y guion bajo, hasta 20 caracteres
ID_VALIDO = re.compile(r"^[A-Za-z0-9_-]{1,20}$")
MAX_UBICACION = 60
MAX_CUERPO = 10_000   # bytes: no leemos peticiones más grandes

# La Central pone aquí una función que se llama al dar de baja una estación,
# para echar a su monitor si está conectado. Así no hace falta que este
# fichero importe WM_Central (se importarían el uno al otro)
al_dar_de_baja = None


# True si la ruta es del Registry (la Central lo mira antes que nada)
def es_ruta_registry(ruta):
    return ruta == PREFIJO or ruta.startswith(PREFIJO + "/")


# IP de quien hace la petición. En Railway las peticiones llegan a través de su
# proxy, que pone la IP real del cliente en la cabecera X-Forwarded-For
# ("ip_cliente, ip_proxy1, ..."); si no está, usamos la de la conexión
def ip_cliente(handler):
    reenviada = handler.headers.get("X-Forwarded-For", "")
    if reenviada:
        return reenviada.split(",")[0].strip()
    return handler.client_address[0]


def responder(handler, codigo, datos):
    handler._responder(codigo, "application/json; charset=utf-8", json.dumps(datos, ensure_ascii=False))


def error(handler, codigo, mensaje):
    responder(handler, codigo, {"error": mensaje})


# Punto de entrada: la Central llama aquí con la petición y el método
def atender(handler, metodo):
    ruta = handler.path.split("?")[0].rstrip("/")   # sin "?..." y sin "/" final
    try:
        if ruta == RUTA_LISTA and metodo == "POST":
            return alta(handler)
        coincide = RUTA_UNA.match(ruta)
        if coincide and metodo == "GET":
            return consultar(handler, coincide.group(1).upper())
        if coincide and metodo == "DELETE":
            return baja(handler, coincide.group(1).upper())
        if ruta == RUTA_LISTA or coincide:
            return error(handler, 405, f"Método {metodo} no permitido en {ruta}")
        return error(handler, 404, "Ruta no encontrada en el Registry")
    except Exception as e:
        # Cualquier fallo inesperado: 500 y a la auditoría, sin tirar la Central
        auditar("ERROR", f"Error en el Registry: {e}", nivel="ERROR", quien="REGISTRY",
                ip=ip_cliente(handler), datos={"funcion": "WM_Registry.atender", "metodo": metodo,
                                               "ruta": ruta, "error": str(e)})
        return error(handler, 500, "Error interno del Registry")


# POST /registry/estaciones
def alta(handler):
    ip = ip_cliente(handler)

    # 1. Leer el cuerpo y convertirlo de JSON a diccionario
    try:
        longitud = int(handler.headers.get("Content-Length", 0))
    except ValueError:
        return error(handler, 400, "Content-Length no válido")
    if longitud > MAX_CUERPO:
        return error(handler, 413, "Petición demasiado grande")
    try:
        datos = json.loads(handler.rfile.read(longitud) or b"{}")
    except (ValueError, UnicodeDecodeError):
        return error(handler, 400, "El cuerpo tiene que ser JSON: {\"id\": \"WS-05\", \"ubicacion\": \"...\"}")
    if not isinstance(datos, dict):
        return error(handler, 400, "El cuerpo tiene que ser un objeto JSON")

    # 2. Comprobar los campos
    ws_id = str(datos.get("id", "")).strip().upper()
    ubicacion = str(datos.get("ubicacion", "")).strip()
    if not ID_VALIDO.match(ws_id):
        return error(handler, 400, "id no válido (letras, números, - y _, hasta 20 caracteres)")
    if not ubicacion or len(ubicacion) > MAX_UBICACION:
        return error(handler, 400, f"ubicacion obligatoria (hasta {MAX_UBICACION} caracteres)")
    if "#" in ubicacion:
        # El monitor manda REG#id#ubicacion: un # dentro de la ubicación rompería ese mensaje
        return error(handler, 400, "La ubicación no puede contener '#'")

    # 3. Darla de alta
    resultado = registrar_estacion(ws_id, ubicacion)
    if resultado == "ya_registrada":
        auditar("REGISTRO_ALTA", f"Alta rechazada: {ws_id} ya está registrada", nivel="AVISO",
                quien=ws_id, ip=ip, datos={"ubicacion": ubicacion, "resultado": resultado})
        return error(handler, 409, f"La estación {ws_id} ya está registrada")

    auditar("REGISTRO_ALTA", f"{ws_id} dada de alta en el Registry ({ubicacion})",
            quien=ws_id, ip=ip, datos={"ubicacion": ubicacion, "resultado": resultado})
    print(f"[REGISTRY] Alta de {ws_id} ({ubicacion}) desde {ip}: {resultado}")
    return responder(handler, 201, {"id": ws_id, "ubicacion": ubicacion, "registrada": True})


# GET /registry/estaciones/{id}
def consultar(handler, ws_id):
    registro = get_registro_estacion(ws_id)
    if registro is None:
        return error(handler, 404, f"La estación {ws_id} no existe")
    _, ubicacion, registrada = registro
    return responder(handler, 200, {"id": ws_id, "ubicacion": ubicacion, "registrada": registrada})


# DELETE /registry/estaciones/{id}
def baja(handler, ws_id):
    ip = ip_cliente(handler)
    if not baja_estacion(ws_id):
        return error(handler, 404, f"La estación {ws_id} no está registrada")
    auditar("REGISTRO_BAJA", f"{ws_id} dada de baja en el Registry", nivel="AVISO", quien=ws_id, ip=ip)
    print(f"[REGISTRY] Baja de {ws_id} desde {ip}")
    # Si su monitor está conectado, la Central lo desconecta: ya no puede seguir dando servicio
    if al_dar_de_baja:
        al_dar_de_baja(ws_id)
    return responder(handler, 200, {"id": ws_id, "registrada": False})
