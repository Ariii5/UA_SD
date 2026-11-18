# ============================================================
#  auditoria.py
#  Registro de auditoría de la Central.
#
#  Cada evento es una línea JSON en auditoria.log, que va en la misma carpeta
#  que la base de datos (DATA_DIR de db_manager.py; en Railway es el volumen
#  /app/data, así el log no se pierde al redesplegar).
#
#  Además se guardan los últimos 500 eventos en memoria para poder
#  devolverlos rápido en GET /eventos sin tener que leer el fichero.
#
#  Campos de cada evento:
#    seq    -> número que sube de 1 en 1 (sigue donde se quedó al reiniciar)
#    fecha  -> fecha y hora con zona horaria (ISO 8601)
#    nivel  -> INFO, AVISO o ERROR
#    tipo   -> qué ha pasado, en corto (AUTH_OK, AUTH_FALLO, FUGA, BLOQUEO...)
#    quien  -> quién lo produce (WS-01, FO-02, CENTRAL, WM_W...)
#    ip     -> desde dónde (IP de la WS o del operario; "-" si no aplica)
#    texto  -> la frase de la noticia, para leerla una persona
#    datos  -> parámetros extra (diccionario)
# ============================================================
import json
import os
import sys
import threading
import collections
from datetime import datetime
from db_manager import DATA_DIR

LOG_PATH = os.path.join(DATA_DIR, "auditoria.log")
NIVELES = ("INFO", "AVISO", "ERROR")
MAX_EN_MEMORIA = 500

# Lo usan varios hilos (sockets, Kafka, web...), por eso va con lock:
# así no se repite ningún seq y las líneas del fichero no se mezclan
lock_auditoria = threading.Lock()
eventos = collections.deque(maxlen=MAX_EN_MEMORIA)   # al llegar a 500 se borran los más viejos
ultimo_seq = 0


# Al arrancar leemos el fichero (si existe) para seguir con el seq donde se
# quedó y para que GET /eventos tenga los últimos 500 aunque Central se reinicie
def _cargar_fichero():
    global ultimo_seq
    if not os.path.exists(LOG_PATH):
        return
    with open(LOG_PATH, "r", encoding="utf-8") as f:
        for linea in f:
            try:
                evento = json.loads(linea)
                eventos.append(evento)
                ultimo_seq = max(ultimo_seq, int(evento["seq"]))
            except (ValueError, KeyError, TypeError):
                pass   # una línea rota (p. ej. si se cortó la luz a medias) no impide arrancar


# Registra un evento. Devuelve el evento tal como se ha guardado.
# Ejemplo:
#   auditar("AUTH_FALLO", "Contraseña incorrecta para WS-03",
#           nivel="AVISO", quien="WS-03", ip="192.168.1.20", datos={"intento": 2})
def auditar(tipo, texto, nivel="INFO", quien="CENTRAL", ip="-", datos=None):#tipo y texto son los unicos obligatorios
    #los demás tienen valor por defecto
    global ultimo_seq #hace falta porque la función modifica ultimo_seq
    nivel = str(nivel).upper() #pasa a nuevel mayúscula nivel
    if nivel not in NIVELES: #si nuvel no es ni INFO, ni AVISO ni ERROR da error
        raise ValueError(f"Nivel de auditoría no válido: {nivel} (tiene que ser INFO, AVISO o ERROR)")

    with lock_auditoria: #a esta función la llaman a la vez el hilo de cada monitor(el de kafka y el de la web), sin el lock los dos 
        #podrian leer ultimo_seq=5 al mimso tieempo un 6. Asi uno entra y el tro espera
        ultimo_seq += 1
        evento = {
            "seq": ultimo_seq,
            "fecha": datetime.now().astimezone().isoformat(timespec="seconds"), 
            "nivel": nivel, 
            "tipo": str(tipo),
            "quien": str(quien),
            "ip": str(ip),
            "texto": str(texto),
            "datos": datos if datos is not None else {},
        }
        eventos.append(evento) #mete el evento en la lista de memoria de 500

        # default=str: si en datos va algo que no es JSON (una fecha, un objeto),
        # se guarda como texto en vez de romper la Central
        linea = json.dumps(evento, ensure_ascii=False, default=str) #recibe un diccionario y devuelve un string
        try:
            with open(LOG_PATH, "a", encoding="utf-8") as f: #abre un fichero y devuelve un objeto para leerlo o escribirlo
                #a--> lo conserva y escriba al final
                f.write(linea + "\n")
        except OSError as e:
            # Si no se puede escribir el fichero, el evento sigue en memoria y la Central sigue
            print(f"[AUDITORIA] No se pudo escribir en {LOG_PATH}: {e}", file=sys.stderr)

    return evento


# Para GET /eventos: copia de los eventos en memoria (del más viejo al más nuevo).
# Con desde=N solo devuelve los que tienen seq mayor que N (para que el Front
# pida solo los nuevos)
def obtener_eventos(desde=0):
    with lock_auditoria:
        return [e for e in eventos if e["seq"] > desde]



# El último seq que se ha dado (0 si todavía no hay ninguno)
def ultimo_seq_auditoria():
    with lock_auditoria:
        return ultimo_seq


_cargar_fichero()
