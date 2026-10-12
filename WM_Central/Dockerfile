FROM python:3.10-slim

# Evitar que Python almacene en búfer la salida de consola (logs inmediatos)
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Librería de Kafka (coordinación de riego)
RUN pip install --no-cache-dir kafka-python==2.2.15

# Copiar scripts del módulo central
COPY db_manager.py .
COPY WM_Central.py .

# Directorio destinado a alojar la base de datos de forma persistente
VOLUME ["/app/data"]

# Puerto TCP de escucha para los monitores de estación
EXPOSE 8000

# Ejecución por defecto: puerto 8000 y broker local (sustituible en despliegue)
CMD ["python3", "WM_Central.py", "8000", "localhost:9092"]