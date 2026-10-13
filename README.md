# Practica-WaterManagement: Sistema Distribuido de Gestión de Riego Inteligente

Sistema distribuido diseñado para la monitorización, telecontrol y diagnóstico en tiempo real de estaciones de riego en parques y jardines públicos. Emplea una arquitectura de comunicación híbrida basada en sockets TCP para el control punto a punto de estaciones y Apache Kafka para la mensajería distribuida asíncrona.

---

## 🤖 Uso de Inteligencia Artificial

En cumplimiento de la normativa de la asignatura, el registro de las consultas y prompts empleados como apoyo durante el diseño, desarrollo y depuración de la práctica se encuentra documentado en el archivo [`prompts_ia.md`].

---

## Arquitectura de Componentes

- **`WM_Central`**: Núcleo central del sistema. Aloja la base de datos relacional (SQLite), el panel de control y un servidor de sockets TCP concurrente (multihilo) responsable de la autenticación de estaciones, control de ciclo de vida (`DISPONIBLE`, `AVERIADA`, `DESCONECTADA`,`REGANDO`) y recepción de alertas en tiempo real. Dispone de configuración Docker para su despliegue aislado.
- **`WM_WS_M` (Monitor de Estación)**: Proceso guardián local con arquitectura dual concurrente:
  - **Cliente TCP frente a `WM_Central`**: Gestiona el apretón de manos (`<ENQ>` / `<ACK>`), la autenticación (`REG#<id>#<ubicacion>`) y el canal de alertas críticas (`ALERT#FUGA`).
  - **Servidor TCP frente al Engine (`WM_WS_E`)**: Monitorea de forma periódica la integridad operativa del actuador mediante pulsos de salud cada segundo (`HEALTH?`).
- **`WM_WS_E` (Engine de Estación)**: Simulación física de la electroválvula y caudalímetro. Conectado por sockets al monitor local para responder a los chequeos de salud (`OK` / `KO`) y por Kafka a la red de telemetría.
- **`WM_FO` (Operario de Campo)**: Aplicación cliente para solicitud, inspección y control de riegos en las zonas verdes.

---

## Protocolo de Comunicación por Sockets

| Fase                 | Mensaje / Trama                     | Sentido                       | Descripción                                                                  |
| -------------------- | ----------------------------------- | ----------------------------- | ---------------------------------------------------------------------------- |
| **Handshake**        | `\x05` (`<ENQ>`) / `\x06` (`<ACK>`) | Monitor $\rightarrow$ Central | Verificación de disponibilidad del canal físico TCP.                         |
| **Autenticación**    | `REG#<id>#<ubicacion>`              | Monitor $\rightarrow$ Central | Envío de credenciales. Valida contra SQLite y pasa el estado a `DISPONIBLE`. |
| **Sondeo de Salud**  | `HEALTH?`                           | Monitor $\rightarrow$ Engine  | Sondeo cada 1s desde el monitor hacia el motor local.                        |
| **Respuesta Salud**  | `OK` / `KO`                         | Engine $\rightarrow$ Monitor  | Respuesta del estado del motor. `KO` indica avería forzada/fuga.             |
| **Reporte de Fallo** | `ALERT#FUGA`                        | Monitor $\rightarrow$ Central | Notificación de avería. Central pasa la estación a `AVERIADA` en BD.         |
| **Desconexión**      | Cierre de socket                    | Monitor $\rightarrow$ Central | Detectada por el servidor; actualiza el registro en BD a `DESCONECTADA`.     |

---

## Guía de Ejecución Local

### Requisitos previos

- Python 3.10 o superior
- Docker Desktop (en ejecución para Apache Kafka y contenedorización de Central)

---

Decisiones de diseño:
- La clave de cada mensaje es el ID de la estación, para que todos sus eventos lleguen en orden (misma partición).
- Cada Engine y cada operario usan un `group_id` propio para recibir **todos** los mensajes del topic y filtrar los suyos.
- Central procesa peticiones y telemetría en un único hilo consumidor, de modo que dos peticiones simultáneas a la misma estación no pueden ser autorizadas a la vez.
- Los mensajes con más de 30 s de antigüedad se descartan (evita ejecutar órdenes antiguas tras un reinicio).

### 1. Inicializar la Central (`WM_Central`)

Inicializa la base de datos relacional SQLite y arranca la escucha del servidor de sockets multihilo en el puerto indicado:

```bash
python3 WM_Central/WM_Central.py 8000 localhost:9092
```
### Ejecución local (Windows)

Con Kafka arrancado (`cd Kafka` y `docker compose up -d`), en terminales separadas desde la raíz del proyecto:

```powershell
py WM_Central\WM_Central.py 8000 localhost:9092
py WM_WS\WM_WS_M.py 127.0.0.1 0 5001 127.0.0.1 8000 WS-01 "River Park"
py WM_WS\WM_WS_E.py localhost:9092 127.0.0.1 5001
py WM_FO\WM_FO.py --broker localhost:9092 --id FO-01 --fichero WM_FO\solicitudes.txt
py WM_FO\WM_FO.py --broker localhost:9092 --id FO-02
```

Formato de `solicitudes.txt`: una petición por línea, `<ID_ESTACION>,<DURACION_EN_SEGUNDOS>`.

### Ejecución con Docker

```powershell
docker build -t wm_central WM_Central
docker build -t wm_ws WM_WS
docker build -t wm_fo WM_FO
docker run -d --name central --network wm_net -p 8000:8000 wm_central python3 WM_Central.py 8000 broker:29092
docker run -it --rm --name ws01 --network wm_net -e BROKER=broker:29092 -e CENTRAL_IP=central -e WS_ID=WS-01 -e UBICACION="River Park" wm_ws
docker run -it --rm --network wm_net wm_fo --broker broker:29092 --id FO-01 --fichero solicitudes.txt
```
