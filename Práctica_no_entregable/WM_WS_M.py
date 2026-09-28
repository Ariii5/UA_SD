import socket
import sys

FORMAT = 'utf-8'

def registrar_estacion(host, puerto, id_estacion, ubicacion):
    try:
        # 1. Crear el socket y conectar con WM_Central
        cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente.connect((host, puerto))
        print(f"[CONECTADO] Conectado a WM_Central en {host}:{puerto}")
        
        # 2. Generar la trama con el formato REGISTRO#<ID>#<UBICACION>
        trama = f"REGISTRO#{id_estacion}#{ubicacion}"
        print(f"[ENVIANDO] {trama}")
        cliente.send(trama.encode(FORMAT))
        
        # 3. Recibir la respuesta del servidor
        respuesta = cliente.recv(1024).decode(FORMAT)
        print(f"[RESPUESTA CENTRAL] {respuesta}")
        
        # 4. Cierre limpio de la conexion
        cliente.close()
        print("[DESCONECTADO] Conexion cerrada limpiamente.")
        
    except Exception as e:
        print(f"[ERROR] No se pudo conectar con el servidor: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Uso: python WM_WS_M.py <host> <puerto> <ID_ESTACION> <UBICACION>")
        print('Ejemplo: python WM_WS_M.py localhost 5050 WS-04 "River Park"')
        sys.exit(1)
        
    host_servidor = sys.argv[1]
    puerto_servidor = int(sys.argv[2])
    estacion_id = sys.argv[3]
    estacion_ubi = sys.argv[4]

    registrar_estacion(host_servidor, puerto_servidor, estacion_id, estacion_ubi)