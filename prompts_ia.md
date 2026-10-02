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
