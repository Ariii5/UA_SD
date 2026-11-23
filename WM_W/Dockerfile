# Imagen de la oficina meteorológica (WM_W)
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir requests
COPY WM_W.py ciudades.json ./
# La API key va en "docker run" con -e OPENWEATHER_KEY=...
ENTRYPOINT ["python", "WM_W.py"]
