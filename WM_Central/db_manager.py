# ============================================================
#  db_manager.py
#  Todo lo que tiene que ver con la base de datos SQLite de Central.
#  Hay dos tablas: estaciones (id, ubicacion, estado) y operarios (id, nombre).
#  Cada función abre la conexión, hace su consulta y la cierra.
# ============================================================
import sqlite3
import os

# En Railway la BD va en /app/data, que es donde está montado el volumen
# (así no se borra al redesplegar). En local se guarda junto a este fichero.
DATA_DIR = "/app/data" if os.path.exists("/app/data") else os.path.dirname(__file__)
DB_PATH = os.path.join(DATA_DIR, "water_management.db")


# ------------------------------------------------------------
#  Crear las tablas y los datos de ejemplo
# ------------------------------------------------------------

# Se llama una vez al arrancar Central
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Tabla de estaciones de riego
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS estaciones (
            id TEXT PRIMARY KEY,
            ubicacion TEXT NOT NULL,
            estado TEXT NOT NULL DEFAULT 'DESCONECTADA'
        )
    """)

    # Tabla de operarios
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS operarios (
            id TEXT PRIMARY KEY,
            nombre TEXT NOT NULL
        )
    """)

    # Estaciones de ejemplo. Con INSERT OR IGNORE, si ya existen no da error
    estaciones_iniciales = [
        ("WS-01", "River Park", "DESCONECTADA"),
        ("WS-02", "Riverside Gardens", "DESCONECTADA"),
        ("WS-03", "Central Park", "DESCONECTADA"),
        ("WS-04", "North Roundabout", "DESCONECTADA")
    ]
    cursor.executemany(
        "INSERT OR IGNORE INTO estaciones (id, ubicacion, estado) VALUES (?, ?, ?)",
        estaciones_iniciales
    )

    # Operarios de ejemplo
    operarios_iniciales = [
        ("FO-01", "Sara Garcia"),
        ("FO-02", "Ariadna")
    ]
    cursor.executemany(
        "INSERT OR IGNORE INTO operarios (id, nombre) VALUES (?, ?)",
        operarios_iniciales
    )

    # Al arrancar, Central no sabe cómo está ninguna estación hasta que su
    # monitor se conecte, así que todas empiezan en DESCONECTADA (gris)
    cursor.execute("UPDATE estaciones SET estado = 'DESCONECTADA'")

    conn.commit()
    conn.close()
    print("[BD] Base de datos inicializada correctamente.")


# ------------------------------------------------------------
#  Consultas
# ------------------------------------------------------------

# Lista de todas las estaciones: [(id, ubicacion, estado), ...]
def get_estaciones():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, ubicacion, estado FROM estaciones")
    filas = cursor.fetchall()
    conn.close()
    return filas


# Lista de todos los operarios: [(id, nombre), ...]
def get_operarios():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, nombre FROM operarios")
    filas = cursor.fetchall()
    conn.close()
    return filas


# Comprueba que existe una estación con ese id Y esa ubicación.
# Es lo que usa Central cuando un monitor manda REG#id#ubicacion
def autenticar_estacion(ws_id, ubicacion):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM estaciones WHERE id = ? AND ubicacion = ?", (ws_id, ubicacion))
    fila = cursor.fetchone()
    conn.close()
    # Si no hay fila, o el id no existe o la ubicación no coincide
    return fila is not None


# Cambia el estado de una estación (DISPONIBLE, REGANDO, AVERIADA...)
def actualizar_estado_estacion(ws_id, nuevo_estado):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE estaciones SET estado = ? WHERE id = ?", (nuevo_estado, ws_id))
    conn.commit()
    conn.close()


# Devuelve el estado de una estación, o None si no existe
def get_estado_estacion(ws_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT estado FROM estaciones WHERE id = ?", (ws_id,))
    fila = cursor.fetchone()
    conn.close()
    return fila[0] if fila else None


# ------------------------------------------------------------
#  Altas de estaciones y operarios (desde la consola o la web)
# ------------------------------------------------------------

# Si la estación no existe la crea en DESCONECTADA; si ya existe le cambia
# la ubicación. Devuelve "creada" o "actualizada" para mostrarlo en pantalla
def alta_estacion(ws_id, ubicacion):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM estaciones WHERE id = ?", (ws_id,))
    if cursor.fetchone():
        cursor.execute("UPDATE estaciones SET ubicacion = ? WHERE id = ?", (ubicacion, ws_id))
        resultado = "actualizada"
    else:
        cursor.execute("INSERT INTO estaciones (id, ubicacion, estado) VALUES (?, ?, 'DESCONECTADA')",
                       (ws_id, ubicacion))
        resultado = "creada"
    conn.commit()
    conn.close()
    return resultado


# Igual que la anterior pero para operarios. Devuelve "creado" o "actualizado"
def alta_operario(op_id, nombre):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM operarios WHERE id = ?", (op_id,))
    if cursor.fetchone():
        cursor.execute("UPDATE operarios SET nombre = ? WHERE id = ?", (nombre, op_id))
        resultado = "actualizado"
    else:
        cursor.execute("INSERT INTO operarios (id, nombre) VALUES (?, ?)", (op_id, nombre))
        resultado = "creado"
    conn.commit()
    conn.close()
    return resultado


# True si el operario está dado de alta
def existe_operario(op_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM operarios WHERE id = ?", (op_id,))
    fila = cursor.fetchone()
    conn.close()
    return fila is not None


# ------------------------------------------------------------
#  Pruebas rápidas: python3 db_manager.py
# ------------------------------------------------------------
if __name__ == "__main__":
    init_db()

    print("\n--- PRUEBA 1: Autenticación ---")
    ok = autenticar_estacion("WS-01", "River Park")
    fail = autenticar_estacion("WS-01", "Ubicacion Falsa")
    print(f"Prueba WS-01 con 'River Park' (debe dar True): {ok}")
    print(f"Prueba WS-01 con 'Ubicacion Falsa' (debe dar False): {fail}")

    print("\n--- PRUEBA 2: Actualización de estado ---")
    print("Cambiando WS-01 a DISPONIBLE...")
    actualizar_estado_estacion("WS-01", "DISPONIBLE")

    for est in get_estaciones():
        if est[0] == "WS-01":
            print(f"Nuevo estado en BD para WS-01: {est[2]} (debe ser DISPONIBLE)")

    # La dejamos otra vez en DESCONECTADA
    actualizar_estado_estacion("WS-01", "DESCONECTADA")
    print("Estado restaurado a DESCONECTADA.")
