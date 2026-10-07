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
