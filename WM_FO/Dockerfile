# Imagen del operario de campo (WM_FO)
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.2.15
COPY WM_FO.py solicitudes.txt ./
# Los argumentos (--broker, --id y opcionalmente --fichero) se añaden en "docker run"
ENTRYPOINT ["python", "WM_FO.py"]
