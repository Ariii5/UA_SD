# ============================================================
#  WM_W.py - Weather Control Office (oficina meteorológica)
#
#  Cada 4 s consulta en OpenWeather el clima de la ciudad de cada estación
#  y avisa si hay HELADA (menos de 0 grados) o LLUVIA.
#  (De momento solo lo enseña por pantalla; mandárselo a Central por
#  API_Central va en la siguiente entrega.)
#
#  Las ciudades están en ciudades.json y se pueden cambiar sin reiniciar:
#  editando el fichero o desde el menú (escribiendo "WS-01 Teruel").
#
#  La API key de OpenWeather se lee de la variable de entorno OPENWEATHER_KEY
#  (nunca en el código).
#
#  Uso: py WM_W.py [--ciudades ciudades.json]
# ============================================================
import os
import sys
import json
import time
import argparse
import threading
import requests

INTERVALO = 4    #segundos entre consultas (lo pide el enunciado)
URL_OPENWEATHER = os.environ.get("OPENWEATHER_URL", "https://api.openweathermap.org/data/2.5/weather")
LLUVIOSOS = ("Rain", "Drizzle", "Thunderstorm")

fichero_ciudades = None
lock_ciudades = threading.Lock()
ultimo_clima = {}       #lo último consultado de cada estación: {"WS-01": {...}}


#Lee el fichero cada vez: si alguien lo edita a mano, el cambio se nota en la siguiente vuelta
def leer_ciudades():
    with lock_ciudades:
        try:
            with open(fichero_ciudades, encoding="utf-8") as f:
                return {ws.upper(): ciudad for ws, ciudad in json.load(f).items()}
        except (OSError, ValueError) as e:
            print(f"[WM_W] X No se puede leer {fichero_ciudades}: {e}")
            return {}


def guardar_ciudad(ws_id, ciudad):
    ciudades = leer_ciudades()
    ciudades[ws_id] = ciudad
    with lock_ciudades:
        with open(fichero_ciudades, "w", encoding="utf-8") as f:
            json.dump(ciudades, f, indent=2, ensure_ascii=False)


#Pide el clima de una ciudad. Devuelve un diccionario con temperatura,
#descripción y alerta (HELADA, LLUVIA o None), o con "error" si no se puede
def consultar_clima(ciudad, clave):
    try:
        r = requests.get(URL_OPENWEATHER, params={"q": ciudad, "appid": clave, "units": "metric"}, timeout=3)
    except requests.RequestException:
        return {"ciudad": ciudad, "error": "OpenWeather no responde"}
    if r.status_code == 404:
        return {"ciudad": ciudad, "error": f"la ciudad {ciudad} no existe"}
    if r.status_code == 401:
        return {"ciudad": ciudad, "error": "API key de OpenWeather incorrecta (o todavía no activada)"}
    if r.status_code == 429:
        return {"ciudad": ciudad, "error": "demasiadas consultas (límite del plan gratis)"}
    if r.status_code != 200:
        return {"ciudad": ciudad, "error": f"OpenWeather contesta {r.status_code}"}
    j = r.json()
    temperatura = j["main"]["temp"]
    principal = j.get("weather", [{}])[0].get("main", "")
    descripcion = j.get("weather", [{}])[0].get("description", "")
    if temperatura < 0:
        alerta = "HELADA"
    elif "rain" in j or principal in LLUVIOSOS:
        alerta = "LLUVIA"
    else:
        alerta = None
    return {"ciudad": ciudad, "temperatura": temperatura, "descripcion": descripcion, "alerta": alerta}


#Texto corto de lo que dice el clima de una estación
def resumen(c):
    if "error" in c:
        return f"ERROR: {c['error']}"
    texto = f"{c['temperatura']} grados, {c['descripcion']}"
    return texto + (f"  <- {c['alerta']}" if c["alerta"] else "")


#Una vuelta: consulta cada ciudad una sola vez aunque la tengan varias
#estaciones (el plan gratis de OpenWeather deja 60 consultas por minuto)
#y enseña lo que ha cambiado
def comprobar(clave):
    ciudades = leer_ciudades()
    consultas = {ciudad: consultar_clima(ciudad, clave) for ciudad in set(ciudades.values())}
    for ws_id, ciudad in ciudades.items():
        c = consultas[ciudad]
        anterior = ultimo_clima.get(ws_id)
        ultimo_clima[ws_id] = c
        #solo se imprime cuando cambia la alerta, la ciudad o el error
        if anterior and (anterior.get("ciudad"), anterior.get("alerta"), anterior.get("error")) == \
                (c.get("ciudad"), c.get("alerta"), c.get("error")):
            continue
        if "error" in c:
            print(f"[WM_W] X {ws_id} ({ciudad}): {c['error']}")
        elif c["alerta"]:
            print(f"[WM_W] ALERTA {c['alerta']} en {ws_id} ({ciudad}, {c['temperatura']} grados)")
        else:
            print(f"[WM_W] Clima bueno en {ws_id} ({ciudad}, {c['temperatura']} grados)")


def bucle_clima(clave):
    while True:
        try:
            comprobar(clave)
        except Exception as e:   #un error raro no puede parar la oficina
            print(f"[WM_W] Error en la consulta: {e}")
        time.sleep(INTERVALO)


def mostrar_estado():
    print("\n  ESTACIÓN  CIUDAD               CLIMA")
    for ws_id, ciudad in sorted(leer_ciudades().items()):
        c = ultimo_clima.get(ws_id)
        print(f"  {ws_id:<9} {ciudad:<20} {resumen(c) if c else 'sin consultar todavía'}")


def menu():
    print("Escribe '<ID> <ciudad>' para cambiar la ciudad (ej: WS-01 Teruel), 'v' para ver el clima o 'q' para salir.")
    while True:
        try:
            linea = input().strip()
        except EOFError:
            return
        if not linea:
            continue
        if linea.lower() == "q":
            return
        if linea.lower() == "v":
            mostrar_estado()
            continue
        partes = linea.split(maxsplit=1)
        if len(partes) != 2:
            print("[WM_W] Formato: <ID> <ciudad>, por ejemplo WS-01 Teruel")
            continue
        ws_id, ciudad = partes[0].upper(), partes[1].strip()
        guardar_ciudad(ws_id, ciudad)
        print(f"[WM_W] {ws_id} ahora está en {ciudad}. Se nota en la siguiente consulta.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Oficina meteorológica (WM_W)")
    parser.add_argument("--ciudades", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "ciudades.json"),
                        help="Fichero JSON con la ciudad de cada estación")
    args = parser.parse_args()
    fichero_ciudades = args.ciudades

    clave = os.environ.get("OPENWEATHER_KEY")
    if not clave:
        print("[WM_W] X Falta la variable de entorno OPENWEATHER_KEY con la API key de OpenWeather.")
        sys.exit(1)

    print("=" * 50)
    print("   OFICINA METEOROLÓGICA (WM_W)")
    print(f"   Ciudades: {fichero_ciudades}")
    print("=" * 50)
    threading.Thread(target=bucle_clima, args=(clave,), daemon=True).start()
    try:
        menu()
    except KeyboardInterrupt:
        pass
    print("\n[WM_W] Saliendo.")