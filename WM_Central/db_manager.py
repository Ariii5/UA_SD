# WM_Central/db_manager.py
import sqlite3
import os

DATA_DIR = "/app/data" if os.path.exists("/app/data") else os.path.dirname(__file__)
DB_PATH = os.path.join(DATA_DIR, "water_management.db")

def init_db():
    #Crea las tablas y carga datos de prueba si no existen.
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Tabla para las estaciones de riego (WS)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS estaciones (
            id TEXT PRIMARY KEY,
            ubicacion TEXT NOT NULL,
            estado TEXT NOT NULL DEFAULT 'DESCONECTADA'
        )
    """)
    
    # Tabla para los operarios de campo (FO)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS operarios (
            id TEXT PRIMARY KEY,
            nombre TEXT NOT NULL
        )
    """)
    
    # Datos iniciales de prueba
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
    
    operarios_iniciales = [
        ("FO-01", "Sara Garcia"),
        ("FO-02", "Ariadna")
    ]
    cursor.executemany(
        "INSERT OR IGNORE INTO operarios (id, nombre) VALUES (?, ?)", 
        operarios_iniciales
    )

    cursor.execute("UPDATE estaciones SET estado = 'DESCONECTADA'")
    
    conn.commit()
    conn.close()
    print("[BD] Base de datos inicializada correctamente.")

def get_estaciones():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, ubicacion, estado FROM estaciones")
    filas = cursor.fetchall()
    conn.close()
    return filas

def get_operarios():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, nombre FROM operarios")
    filas = cursor.fetchall()
    conn.close()
    return filas


#comprueba si la estación y la ubicación existen en la base de datos
def autenticar_estacion(ws_id, ubicacion):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM estaciones WHERE id = ? AND ubicacion = ?", (ws_id, ubicacion))
    fila = cursor.fetchone() #recupera la primera coincidencia tanto en identificador como en nombre de ubicación
    conn.close()
    return fila is not None #si no existe o la ubicación no ccoincide devuelve false

#actualiza el estado de una estación en SQLite
def actualizar_estado_estacion(ws_id, nuevo_estado):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE estaciones SET estado = ? WHERE id = ?", (nuevo_estado, ws_id))
    conn.commit()
    conn.close()

# consultas para la coordinación de riego por Kafka
def get_estado_estacion(ws_id):
    """Devuelve el estado de la estación, o None si no existe."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT estado FROM estaciones WHERE id = ?", (ws_id,))
    fila = cursor.fetchone()
    conn.close()
    return fila[0] if fila else None

def alta_estacion(ws_id, ubicacion):
    """Da de alta una estación nueva (DESCONECTADA hasta que su Monitor se conecte)
    o, si ya existe, actualiza su ubicación. Devuelve 'creada' o 'actualizada'."""
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


def alta_operario(op_id, nombre):
    """Da de alta un operario nuevo o actualiza su nombre. Devuelve 'creado' o 'actualizado'."""
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


def existe_operario(op_id):
    """True si el operario está dado de alta en la BD."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM operarios WHERE id = ?", (op_id,))
    fila = cursor.fetchone()
    conn.close()
    return fila is not None

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
            
    # Restauramos a DESCONECTADA para dejarlo limpio
    actualizar_estado_estacion("WS-01", "DESCONECTADA")
    print("Estado restaurado a DESCONECTADA.")