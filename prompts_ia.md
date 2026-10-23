## Semana 1: Planificación y Arquitectura de Comunicaciones

### Diseño de la topología de Sockets (Roles Cliente/Servidor)

**Fecha:** 30 de septiembre de 2026
**Prompt:**

> "En una estación de monitorización distribuida donde un proceso central supervisa la red y a su vez cada estación tiene un proceso guardián y un actuador físico, ¿cuál es el mejor patrón de sockets? ¿Tiene sentido que el monitor actúe como cliente de la central pero como servidor de su propio actuador local?"
> **Propósito:** Validar la arquitectura de sockets y la necesidad de gestión concurrente (hilos) para atender dos extremos de red simultáneamente.

---

### Consultas técnicas sobre SQLite y tramas de control en Python

**Fecha:** 2 de octubre de 2026
**Prompt:**

> "¿Cómo se gestiona en Python una base de datos SQLite para consultar y actualizar estados concurrentes sin bloquear lecturas de red? ¿Cuál es la forma estándar de manejar caracteres ASCII de control como ENQ (0x05) y ACK (0x06) sobre un socket TCP?"
> **Propósito:** Resolver dudas puntuales de sintaxis con la librería `sqlite3` nativa y la manipulación de bytes de control en la capa de transporte.

---

### Creación de tablas e inserción de datos iniciales en SQLite

**Fecha:** 2 de octubre de 2026
**Prompt:**

> "¿Cómo se define un script en Python con sqlite3 para crear las tablas de estaciones y operarios, y cómo insertar datos de prueba iniciales evitando que den error de duplicados si vuelvo a ejecutar el script?"
> **Propósito:** Implementar la base de datos water_management.db usando sentencias IF NOT EXISTS e INSERT OR IGNORE para arrancar con estaciones de ejemplo en estado desconectada.

---

### Evitar el bloqueo de la terminal al reiniciar el servidor de sockets

**Fecha:** 2 de octubre de 2026
**Prompt:**

> "Cuando paro mi servidor de sockets en Python con Ctrl+C y lo vuelvo a arrancar enseguida, me da el error de que el puerto ya está en uso. ¿Qué línea tengo que añadir para que me deje reutilizar el puerto sin tener que esperar?"
> **Propósito:** Configurar la opción SO_REUSEADDR en el socket de WM_Central para poder reiniciar y depurar rápidamente en local.

---

### Envío y lectura de bytes de control ASCII por un socket

**Fecha:** 2 de octubre de 2026
**Prompt:**

> "¿Cómo se envían caracteres de control como ENQ y ACK por un socket en Python? ¿Tienen que mandarse como texto normal o hay que codificarlos en bytes con formato hexadecimal?"
> **Propósito:** Aprender la sintaxis de bytes en Python (b'\x05' y b'\x06') para realizar correctamente el primer apretón de manos entre WM_WS_M y WM_Central.

---

### Ejecución paralela de servidor y cliente en el mismo archivo

**Fecha:** 2 de octubre de 2026
**Prompt:**

> "Necesito que mi script de monitor sea cliente hacia la central pero a la vez se quede escuchando como servidor a su motor. ¿Cómo uso threading.Thread para que el bucle de escucha no bloquee el resto del programa?"
> **Propósito:** Lanzar la escucha local de WM_WS_M en un hilo secundario tipo daemon mientras el hilo principal realiza la conexión inicial con Central.

---

### Validación básica de argumentos de entrada por consola

**Fecha:** 2 de octubre de 2026
**Prompt:**

> "¿Cómo puedo comprobar en Python que el usuario ha pasado todos los parámetros obligatorios por terminal usando sys.argv y cómo convertir el puerto a número entero para que lo acepte el socket?"
> **Propósito:** Asegurar que los módulos WM_Central y WM_WS_M muestren un mensaje de ayuda claro si faltan argumentos al ejecutarlos.

## Semana 2: Autenticación en BD, Chequeo de Salud, Telemetría de Fallos y Contenerización

### Validación de identidad y actualización de estados en SQLite

**Fecha:** 5 de octubre de 2026  
**Prompt:**

> "¿Cómo implemento en Python con sqlite3 dos funciones seguras: una que valide si coinciden el id y la ubicación de una estación en la base de datos (SELECT) y otra que modifique su estado a DISPONIBLE o AVERIADA (UPDATE) asegurando que los cambios queden guardados en disco?"  
> **Propósito:** Crear `autenticar_estacion` y `actualizar_estado_estacion` en `db_manager.py` usando consultas parametrizadas para evitar inyecciones SQL y asegurar la persistencia con `conn.commit()`.

---

### Protocolo de autenticación por sockets tras el handshake inicial

**Fecha:** 5 de octubre de 2026  
**Prompt:**

> "Una vez superado el handshake inicial con ENQ y ACK, ¿cómo hago para que el cliente envíe una trama con su ID y ubicación separadas por almohadilla (ej. REG#WS-01#River Park), y cómo debe el servidor trocearla con split('#') para validarla contra la base de datos y responder ACK o NACK?"  
> **Propósito:** Definir e implementar el flujo de autenticación de estaciones en `WM_Central.py` y `WM_WS_M.py`, pasando el estado de la estación a DISPONIBLE en SQLite solo si las credenciales existen en la base de datos.

---

### Supervisión periódica de salud (Keep-Alive) con hilos en el Monitor

**Fecha:** 6 de octubre de 2026  
**Prompt:**

> "En el monitor de la estación, ¿cómo programo un hilo en segundo plano con time.sleep(1) que le pregunte continuamente 'HEALTH?' por un socket local a su motor, y cómo puedo reutilizar el socket que ya tengo abierto con Central para mandarle una alerta crítica si el motor falla?"  
> **Propósito:** Desarrollar `health_pulse_worker` en `WM_WS_M.py` para vigilar cada segundo la integridad física del Engine y vincularlo mediante variables compartidas con el canal de Central para emitir `ALERT#FUGA`.

---

### Simulación de fallos mecánicos y lectura de teclado no bloqueante

**Fecha:** 6 de octubre de 2026  
**Prompt:**

> "Quiero crear un simulador ligero del motor (WM_WS_E) que responda 'OK' por socket a los pulsos de salud que recibe, pero que me permita pulsar una tecla como 'k' en la terminal sin pausar ni congelar las respuestas de red. ¿Cómo separo la lectura de teclado con input() en un hilo paralelo?"  
> **Propósito:** Implementar `WM_WS_E.py` con un hilo `keyboard_listener` que altere una bandera booleana para provocar una respuesta `KO` bajo demanda al pulsar la tecla 'k'.

---

### Depuración de excepciones por NameError en argumentos del sistema

**Fecha:** 6 de octubre de 2026  
**Prompt:**

> "Al arrancar mi script cliente me sale el error 'NameError: name sys is not defined' justo en la línea donde compruebo len(sys.argv). ¿Por qué ocurre y cómo lo soluciono?"  
> **Propósito:** Identificar la omisión de la sentencia `import sys` en la cabecera de `WM_WS_E.py` y corregir la inicialización del módulo.

---

### Detección y persistencia de fallos críticos y desconexiones en Central

**Fecha:** 6 de octubre de 2026  
**Prompt:**

> "¿Cómo estructuro el bucle de escucha del servidor en Central para que se quede esperando alertas como 'ALERT#FUGA' sin cerrarse, actualice la base de datos a AVERIADA y, si la estación se desconecta bruscamente o con Ctrl+C, use un bloque finally para marcarla automáticamente como DESCONECTADA?"  
> **Propósito:** Asegurar la consistencia del ciclo de vida de las estaciones en SQLite dentro de `handle_monitor_client` en `WM_Central.py`.

## Semana 3: Panel de Monitorización, Comandos de Administración y Despliegue en Railway

### Diseño de la interfaz de consola sin parpadeo y separación de entrada

**Fecha:** 13 de octubre de 2026  
**Prompt:**

> "En una aplicación servidora en Python donde necesito refrescar periódicamente una tabla con métricas en tiempo real (caudal y volumen) y a la vez permitir la entrada interactiva de comandos del usuario sin que la limpieza de pantalla (`clear`) borre lo que se está tecleando"  
> **Propósito:** Evaluar y seleccionar el enfoque para el panel interactivo de `WM_Central`, adoptando una división en dos ventanas independientes (`HSplit` con `Window` y `TextArea`) para evitar el parpadeo de pantalla.

---

### Coordinación de órdenes administrativas frente a peticiones concurrentes

**Fecha:** 13 de octubre de 2026  
**Prompt:**

> "Para cumplir con la opción de administración de forzar un riego desde la Central, ¿es preferible simular un mensaje de solicitud por el topic de peticiones o emitir directamente la orden de apertura a la cola de órdenes? ¿Cómo se gestionan las estructuras de reservas (`pendientes`) para que un comando manual no colisione con las peticiones habituales de los operarios?"  
> **Propósito:** Diseñar las funciones `admin_forzar_riego`, `admin_bloquear_estacion` y `admin_activar_estacion` en `WM_Central.py`, sincronizando la estructura en memoria y la base de datos SQLite con los topics de Kafka.

---

### Incompatibilidad de directivas VOLUME en plataformas PaaS (Railway)

**Fecha:** 15 de octubre de 2026  
**Prompt:**

> "Al compilar un contenedor en Railway mediante Dockerfile, el proceso falla en la fase de validación con el mensaje 'The Dockerfile failed validation. Please check the build logs... Remove the VOLUME instruction'. ¿Por qué la plataforma rechaza la instrucción VOLUME dentro del Dockerfile y cuál es la forma correcta de adjuntar almacenamiento persistente para SQLite?"  
> **Propósito:** Eliminar la directiva estática `VOLUME` de `WM_Central/Dockerfile` y configurar el punto de montaje persistente directamente sobre la infraestructura del servicio en Railway (`/app/data`).

---

### Adaptación de bucles interactivos TTY para entornos headless en contenedores cloud

**Fecha:** 15 de octubre de 2026  
**Prompt:**

> "Un servicio Python que funciona en local falla en Railway al arrancar con 'Warning: Input is not a terminal (fd=0)', lanzando `selectors.register PermissionError: [Errno 1] Operation not permitted` y `EOFError` dentro de `prompt_toolkit`. ¿Por qué ocurre esto al ejecutarse como contenedor sin consola TTY?"  
> **Propósito:** Desarrollar un arranque tolerante en `WM_Central.py` que detecte la ausencia de terminal interactiva (`sys.stdin.isatty()`) y capture excepciones de registro para pasar de forma transparente a un modo de ejecución continua de sockets y Kafka sin detener el contenedor.

---

### Redirección TCP y configuración de proxies para brokers remotos y clientes locales

**Fecha:** 15 de octubre de 2026  
**Prompt:**

> "Si tengo un servidor de sockets TCP escuchando en el puerto 8000 y un broker Kafka en el puerto interno 9092 dentro de Railway, ¿cómo deben configurarse los TCP Proxies públicos para que clientes locales externos puedan conectarse sin interferir con la comunicación privada entre contenedores de la misma red virtual?"  
> **Propósito:** Establecer los dominios y puertos públicos proxy (`tramway.proxy.rlwy.net:32110` para sockets y `iriguchi.proxy.rlwy.net:57795` para Kafka) asegurando la interoperabilidad con las terminales locales `WM_WS_M`, `WM_WS_E` y `WM_FO`.

---

### Estructura técnica de una guía de despliegue reproducible para memorias de ingeniería

**Fecha:** 15 de octubre de 2026  
**Prompt:**

> "¿Qué apartados esenciales debe contener una guía técnica de despliegue para una memoria de Sistemas Distribuidos con arquitectura híbrida (nube y local)? ¿Cómo se documenta de forma rigurosa la secuencia de construcción, las variables de entorno, la topología de red y las incidencias de ejecución resueltas durante el despliegue?"  
> **Propósito:** Elaborar el borrador formal del apartado de despliegue para la memoria del proyecto, detallando la configuración del contenedor, volúmenes de datos y el protocolo de verificación extremo a extremo.

## Semana 4: Resiliencia, Pruebas de Estrés y Depuración

### Diagnóstico de desconexión de clientes con el broker Kafka desplegado en Railway

**Fecha:** 21 de octubre de 2026  
**Prompt:**

> "Desde que me he bajado los últimos cambios, al conectar WM_WS_E y WM_FO al Kafka de Railway me sale '✖ Imposible conectar con WM_Central / gestor de colas', aunque el Monitor sí se autentica con Central por sockets. ¿Es por algo del código nuevo o por una configuración mal puesta?"  
> **Propósito:** Identificar que los clientes esperan ahora el mensaje `LATIDO` que Central publica en `wm_respuestas`, y que la Central desplegada en Railway seguía ejecutando una versión anterior por tener los builds pausados durante una incidencia de la plataforma.

---

### Timeout en el pulso de salud entre Monitor y Engine

**Fecha:** 21 de octubre de 2026  
**Prompt:**

> "El Monitor tiene que avisar si el Engine cae o no responde. ¿Cómo añado un timeout al recv del pulso de salud para que, si el Engine se cuelga sin cerrar el socket, el Monitor no se quede bloqueado y la estación pase a ROJO en Central?"  
> **Propósito:** Aplicar `settimeout` al socket del Engine en `health_pulse_worker` (`WM_WS_M.py`), capturando `socket.timeout` antes que `OSError` y unificando el aviso a Central en `reportar_averia` para los casos de KO, timeout y cierre de conexión.

---

### Error Broken pipe al reanudar un Engine congelado

**Fecha:** 21 de octubre de 2026  
**Prompt:**

> "Al congelar el Engine con Ctrl+Z y reanudarlo con fg, el programa se cierra con 'Desconexión: [Errno 32] Broken pipe' en vez de reconectarse al Monitor. ¿Por qué pasa y cómo lo arreglo?"  
> **Propósito:** Entender que el Engine leía un `HEALTH?` pendiente en el buffer y fallaba al responder sobre un socket ya cerrado por el Monitor; se captura `OSError` en el `sendall` de `WM_WS_E.py` para que el siguiente `recv` detecte el cierre y active `reconectar_monitor`.

---

### Notificación de avería resuelta y respeto del bloqueo administrativo

**Fecha:** 21 de octubre de 2026  
**Prompt:**

> "El Monitor tiene que seguir vigilando después de un KO y, cuando vuelva el OK, enviar ALERT#RESUELTA para que Central ponga la estación en verde. Además, si estaba bloqueada antes de la avería, al repararse no debería ponerse en verde. ¿Cómo lo organizo?"  
> **Propósito:** Introducir la bandera `estacion_averiada` en `WM_WS_M.py` para avisar una sola vez y emitir `ALERT#RESUELTA`, y el conjunto `bloqueadas_con_averia` en `WM_Central.py` para devolver la estación a FUERA DE SERVICIO en lugar de DISPONIBLE cuando corresponda.

---

### Reconexión automática del Monitor ante la caída de Central

**Fecha:** 23 de octubre de 2026  
**Prompt:**

> "¿Cómo hago que el Monitor se reconecte solo si Central se cae y vuelve, sin tener que reiniciarlo, y que si la estación seguía averiada Central lo sepa al reconectar?"  
> **Propósito:** Crear `mantener_conexion_central` en `WM_WS_M.py`, que escucha el socket de Central, detecta la caída con `recv()` y reintenta cada 3 s con timeout de conexión, reenviando `ALERT#FUGA` tras el registro si la avería persiste.

---

### Estado inicial DESCONECTADA de todas las estaciones al arrancar Central

**Fecha:** 23 de octubre de 2026  
**Prompt:**

> "Al reiniciar Central, las estaciones aparecen con el último estado guardado en SQLite (rojo, naranja...) aunque todavía no se ha conectado ningún Monitor. ¿Cómo hago que arranquen todas en gris como pide el enunciado?"  
> **Propósito:** Añadir en `init_db()` (`db_manager.py`) un `UPDATE estaciones SET estado = 'DESCONECTADA'`, ya que `INSERT OR IGNORE` solo fija el estado la primera vez que se crea la estación.

---

### Coherencia de las órdenes bloquear/activar según el estado de la estación

**Fecha:** 23 de octubre de 2026  
**Prompt:**

> "Al hacer 'bloquear all' se bloquean también las estaciones desconectadas, y con 'activar all' pasan a verde aunque no haya ningún Monitor. Según el enunciado, ¿sobre qué estados deberían actuar estas órdenes? ¿Y cómo muestro en el Monitor las órdenes ORDEN#BLOQUEAR y ORDEN#ACTIVAR que le manda Central?"  
> **Propósito:** Restringir `admin_activar_estacion` a estaciones FUERA DE SERVICIO y `admin_bloquear_estacion` a DISPONIBLE/REGANDO (anotando las AVERIADAS para bloquearlas al repararse), devolviendo un resumen al panel; y procesar en `WM_WS_M.py` las órdenes de Central línea a línea con un buffer.

---

### Diseño y ejecución de pruebas de estrés sobre sockets

**Fecha:** 23 de octubre de 2026  
**Prompt:**

> "Tengo que diseñar y ejecutar pruebas de estrés sobre sockets: cortes abruptos de conexión entre Monitor y Central, reconexiones en caliente y saturación de peticiones concurrentes, contra la Central desplegada en Railway. ¿Qué pruebas hago y cómo las automatizo?"  
> **Propósito:** Definir la batería de pruebas (Ctrl+C, `kill -9`, `kill -STOP`, reinicio de Central en Railway, IDs duplicados) y crear `pruebas/estres_sockets.py` para lanzar conexiones simultáneas, tramas basura, cortes con RST y conexiones ociosas. Se detectaron dos fallos: Central no detecta un Monitor congelado y un ID duplicado provoca una desconexión incorrecta.

---

### Latido del Monitor hacia Central para detectar cortes sin cierre

**Fecha:** 23 de octubre de 2026  
**Prompt:**

> "Si congelo el Monitor con kill -STOP, Central sigue mostrando la estación como DISPONIBLE para siempre. ¿Cómo hago que Central detecte un Monitor colgado o una red cortada sin cierre y la marque como DESCONECTADA?"  
> **Propósito:** Diseñar un latido `PING` cada 2 s desde `WM_WS_M.py` y un timeout de 6 s en el `recv()` de `handle_monitor_client` (`WM_Central.py`), delimitando las tramas con `\n` para que los PING y las alertas no se mezclen en el mismo socket.

---

### Integridad y persistencia de SQLite ante caídas del servidor

**Fecha:** 23 de octubre de 2026  
**Prompt:**

> "¿Cómo compruebo la integridad de SQLite ante caídas del servidor y que las transacciones y estados persistan correctamente, tanto en local como en Railway?"  
> **Propósito:** Matar Central con `kill -9` durante escrituras concurrentes y verificar con `PRAGMA integrity_check` que la base de datos no se corrompe y conserva el último estado escrito, y confirmar que en Railway la BD reside en el volumen montado en `/app/data`.
