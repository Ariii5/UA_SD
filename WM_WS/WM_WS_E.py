#Misión:
# - Responder OK a los chequeos periódicos de salud (HEALTH?)
# - Simular una avería
import sys
import socket
import threading

simular_fuga = False

def keyboard_listener():
    """Escucha la pulsación de teclas en la terminal."""
    global simular_fuga
    while True:
        cmd = input()
        if cmd.strip().lower() == 'k':
            simular_fuga = True
            print("\n[ENGINE] ¡FALLO FORZADO! En el próximo pulso de salud se enviará 'KO'...\n")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python3 WM_WS_E.py <ip_monitor> <puerto_monitor>")
        print("Ejemplo: python3 WM_WS_E.py 127.0.0.1 5001")
        sys.exit(1)

    ip_monitor = sys.argv[1]
    puerto_monitor = int(sys.argv[2])

    print("=" * 50)
    print("      SIMULADOR DEL MOTOR (WM_WS_E)")
    print("=" * 50)

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip_monitor, puerto_monitor))
        print(f"[ENGINE] Conectado al Monitor en {ip_monitor}:{puerto_monitor}")
        print("[ENGINE] Respondiendo 'OK' a los pulsos de salud.")
        print("[ENGINE] >> Escribe 'k' y pulsa Enter para forzar una avería <<\n")

        # Hilo para leer el teclado en paralelo
        threading.Thread(target=keyboard_listener, daemon=True).start()

        while True:
            data = sock.recv(1024)
            if not data:
                break
            
            if data == b"HEALTH?":
                if simular_fuga:
                    sock.sendall(b"KO")
                    print("[ENGINE] Enviado: KO -> Avería reportada.")
                else:
                    sock.sendall(b"OK")
    except ConnectionRefusedError:
        print(f"[ENGINE] Error: El Monitor no está escuchando en {ip_monitor}:{puerto_monitor}")
    except Exception as e:
        print(f"[ENGINE] Desconexión: {e}")
    finally:
        sock.close()