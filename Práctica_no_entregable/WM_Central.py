#Ariadna
import socket
import threading
import sys

FORMAT = 'utf-8'

def handle_station(conn, addr):
    """Hilo encargado de atender a una estacion de riego individual."""
    try:
        # 1. Recibir la trama enviada por el monitor
        mensaje = conn.recv(1024).decode(FORMAT)
        
        if mensaje:
            # 2. Parsear los campos usando el separador '#'
            partes = mensaje.strip().split('#')
            
            # Formato esperado: REGISTRO#<ID_ESTACION>#<UBICACION>
            if len(partes) == 3 and partes[0] == "REGISTRO":
                id_estacion = partes[1]
                ubicacion = partes[2]
                
                print(f"[REGISTRO OK] Estacion {id_estacion} conectada desde {addr} en '{ubicacion}'.")
                
                # 3. Responder confirmacion estructurada
                respuesta = "STATUS#OK#Estacion registrada correctamente"
                conn.send(respuesta.encode(FORMAT))
            else:
                print(f"[ERROR TRAMA] Trama no reconocida de {addr}: {mensaje}")
                respuesta = "STATUS#ERROR#Formato de registro no valido"
                conn.send(respuesta.encode(FORMAT))
                
    except Exception as e:
        print(f"[EXCEPCION] Error atendiendo a {addr}: {e}")
    finally:
        # 4. Cerrar la conexion con este cliente
        conn.close()

def main():
    if len(sys.argv) < 2:
        print("Uso: python WM_Central.py <puerto>")
        print("Ejemplo: python WM_Central.py 5050")
        sys.exit(1)

    puerto = int(sys.argv[1])
    # Escucha en todas las interfaces de red locales
    host = '0.0.0.0'
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Permite reutilizar el puerto inmediatamente tras reiniciar
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, puerto))
    server.listen()
    
    print(f"[WM_Central] Servidor a la escucha en el puerto {puerto}...")

    # Bucle infinito para aceptar multiples conexiones concurrentes
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_station, args=(conn, addr))
        thread.start()
        print(f"[HILOS ACTIVOS] {threading.active_count() - 1}")

if __name__ == "__main__":
    main()