# Practica-WaterManagement: Sistema Distribuido de Gestión de Riego Inteligente

Sistema distribuido diseñado para la monitorización, telecontrol y diagnóstico en tiempo real de estaciones de riego en parques y jardines públicos. Emplea una arquitectura de comunicación híbrida basada en sockets TCP para el control punto a punto de estaciones y Apache Kafka para la mensajería distribuida asíncrona.

---

## 🤖 Uso de Inteligencia Artificial

En cumplimiento de la normativa de la asignatura, el registro de las consultas y prompts empleados como apoyo durante el diseño, desarrollo y depuración de la práctica se encuentra documentado en el archivo [`prompts_ia.md`].

---

## Arquitectura de Componentes

- **`WM_Central`**: Núcleo central del sistema. Aloja la base de datos relacional (SQLite), el panel de control y un servidor de sockets TCP concurrente (multihilo) responsable de la autenticación de estaciones, control de ciclo de vida (`DISPONIBLE`, `AVERIADA`, `DESCONECTADA`) y recepción de alertas en tiempo real. Dispone de configuración Docker para su despliegue aislado.
- **`WM_WS_M` (Monitor de Estación)**: Proceso guardián local con arquitectura dual concurrente:
  - **Cliente TCP frente a `WM_Central`**: Gestiona el apretón de manos (`<ENQ>` / `<ACK>`), la autenticación (`REG#<id>#<ubicacion>`) y el canal de alertas críticas (`ALERT#FUGA`).
  - **Servidor TCP frente al Engine (`WM_WS_E`)**: Monitorea de forma periódica la integridad operativa del actuador mediante pulsos de salud cada segundo (`HEALTH?`).
- **`WM_WS_E` (Engine de Estación)**: Simulación física de la electroválvula, bomba y caudalímetro. Conectado por sockets al monitor local para responder a los chequeos de salud (`OK` / `KO`) y por Kafka a la red de telemetría.
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

### 1. Inicializar la Central (`WM_Central`)

Inicializa la base de datos relacional SQLite y arranca la escucha del servidor de sockets multihilo en el puerto indicado:

```bash
python3 WM_Central/WM_Central.py 8000 localhost:9092
```
