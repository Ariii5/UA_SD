#funciónn principal:
# - estar siempre encendido a la escucha
# - aceptar conexiones simultáneas de múltiples estaciones de riego (WM_WS_M)
# - autenticarlas usando la base de datos SQLite
# - Reaccionar de inmediato si alguna se avería
import sys
import socket
import threading
from db_manager import *

ENQ = b'\x05'   # Solicitud inicial
ACK = b'\x06'   # Confirmación OK
NACK = b'\x15'  # Error / No reconocido

#cada vez que un Monitor se conecta a la central el servidor principal geenera un hilo que ejecuta la funciòn exclusiva para esa conexión
def handle_monitor_client(client_socket, client_address):
    ws_id = None 
    print(f"\n[CENTRAL] Conexión TCP entrante desde {client_address}") #client address--> tupla de la IP y puerto
    try:
        # 1. Handshake inicial
        data = client_socket.recv(1024)#hilo se pausa hasta que llegan bytes por la red
        if data != ENQ: 
            print(f"[CENTRAL] Byte recibido incorrecto ({data}). Enviando <NACK>...")
            client_socket.sendall(NACK)
            return
        
        client_socket.sendall(ACK)
        print(f"[CENTRAL] Handshake superado con {client_address}. Esperando registro...")

        # convierte los bytes entrantes en texto legible mediante 'decode'
        reg_data = client_socket.recv(1024).decode('utf-8').strip()
        #'cada vez que nos envíemos datos separaremos cada dato con una almohadilla
        partes = reg_data.split("#")

        if len(partes) == 3 and partes[0] == "REG":
            ws_id = partes[1]
            ubicacion = partes[2]

            if autenticar_estacion(ws_id, ubicacion):
                actualizar_estado_estacion(ws_id, "DISPONIBLE")
                client_socket.sendall(ACK)
                print(f"[CENTRAL] Estación {ws_id} ({ubicacion}) autenticada con éxito -> Estado: DISPONIBLE")
            else:
                client_socket.sendall(NACK)
                print(f"[CENTRAL] Error: Estación {ws_id} no coincide en BD.")
                return
        else:
            client_socket.sendall(NACK)
            return

        # 3. Mantener la escucha de alertas mientras la estación siga viva
        while True:
            #el hilo se queda escuchando sin gastar ciclos
            msg = client_socket.recv(1024)
            if not msg:
                break
            
            texto = msg.decode('utf-8').strip()
            if texto == "ALERT#FUGA":
                actualizar_estado_estacion(ws_id, "AVERIADA")
                print(f"\n[ALERTA CRÍTICA] ¡Avería/Fuga detectada en {ws_id}! -> Estado en BD: AVERIADA\n")

    except Exception as e:
        print(f"[CENTRAL] Incidencia con {ws_id or client_address}: {e}")
    finally:
        if ws_id:
            actualizar_estado_estacion(ws_id, "DESCONECTADA")
            print(f"[CENTRAL] Estación {ws_id} desconectada -> Estado: DESCONECTADA\n")
        client_socket.close()
    


#actúa como recepcionista de la central, su propósito es poner en marcha el servidor de red, quedarse esperando llamadas entrantes de los monitores
#de ruefi (WM_WS_M) y derivar cada conexión a un hilo independiente
def start_socket_server(port):
    #crea el objeto de socket de red
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #activa la reutilización automática del puerto local.
    #aunque hagas contro + C y lo vuelvas a usar inmediatamente
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    #enlaza el socket a una interfaz física y a un puerto. Al poner 0.0.0.0 le ordena que escuche en todas las tarjetas e interfaces de red del equipo
    server.bind(("0.0.0.0", port))
    #pone el socket en modo de escucha pasiva
    server.listen(5)
    print(f"[CENTRAL] Servidor de Sockets a la escucha en el puerto {port}...")
    
    while True:
        client_sock, addr = server.accept()
        #cre un nuevo hilo de ejecución en paralelo
        thread = threading.Thread(target=handle_monitor_client, args=(client_sock, addr))
        #hilo en segundo plano(demonio), todos los hilos creados se destruyen al instante  sin dejar procesos huérfanos
        thread.daemon = True
        #arranca el hilo
        thread.start()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python3 WM_Central.py <puerto_sockets> <ip_kafka:puerto>")
        print("Ejemplo: python3 WM_Central.py 8000 localhost:9092")
        sys.exit(1)
        
    socket_port = int(sys.argv[1])
    kafka_server = sys.argv[2]
    
    print("=" * 50)
    print("      INICIANDO SISTEMA WM_CENTRAL")
    print("=" * 50)

    init_db()
    print("\n--- Estado inicial de estaciones (BD) ---")
    for est in get_estaciones():
        print(f"[{est[2]}] ID: {est[0]} | Zona: {est[1]}")
    print("-" * 40)

    start_socket_server(socket_port)