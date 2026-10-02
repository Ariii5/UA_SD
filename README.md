## 🤖 Uso de Inteligencia Artificial

En cumplimiento de la normativa de la asignatura, el registro de las consultas y prompts empleados como apoyo durante el diseño, desarrollo y depuración de la práctica se encuentra documentado en el archivo [`prompts_ia.md`].

## Arquitectura de Componentes

- **`WM_Central`**: Núcleo central del sistema. Aloja la base de datos relacional (SQLite), el panel de monitorización y el servidor de sockets TCP para el alta y supervisión de las estaciones.
- **`WM_WS_M` (Monitor de Estación)**: Proceso guardián de cada estación. Actúa como cliente TCP frente a `WM_Central` y como servidor TCP local frente al actuador/motor.
- **`WM_WS_E` (Engine de Estación)**: Simulación física de la electroválvula y caudalímetro. Conectado por sockets al monitor y por Kafka a la red.
- **`WM_FO` (Operario de Campo)**: Aplicación cliente para solicitud e inspección de riegos en las zonas verdes.

---

## Guía de Ejecución Local (Hito Semana 1)

### Requisitos previos

- Python 3.10 o superior
- Docker Desktop (para la instancia local de Apache Kafka)

### 1. Inicializar la Central (`WM_Central`)

Crea y consulta la base de datos SQLite y arranca la escucha por sockets TCP:

```bash
cd WM_Central
python3 WM_Central.py 8000 localhost:9092
```
