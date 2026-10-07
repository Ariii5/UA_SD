# WM_Central/db_manager.py
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "water_management.db")

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