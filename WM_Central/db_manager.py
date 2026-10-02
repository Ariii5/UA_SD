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

if __name__ == "__main__":
    init_db()
    print("\n--- Estaciones registradas ---")
    for est in get_estaciones():
        print(f"ID: {est[0]} | Ubicacion: {est[1]} | Estado: {est[2]}")
        
    print("\n--- Operarios registrados ---")
    for op in get_operarios():
        print(f"ID: {op[0]} | Nombre: {op[1]}")