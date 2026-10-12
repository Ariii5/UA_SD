# Imagen de una estación de riego completa: Monitor (WM_WS_M) + Engine (WM_WS_E)
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.2.15
COPY WM_WS_M.py WM_WS_E.py ./

# Valores por defecto; se cambian en "docker run" con -e NOMBRE=valor
ENV BROKER=localhost:9092 \
    CENTRAL_IP=127.0.0.1 \
    CENTRAL_PUERTO=8000 \
    WS_ID=WS-01 \
    UBICACION="River Park"

# 1) El Monitor arranca en segundo plano (&) y espera al Engine en el puerto 5001.
# 2) 'exec' hace que el Engine sustituya a la shell: así recibe el teclado (docker run -it)
#    y la señal de "docker stop", y cierra la válvula de forma ordenada.
CMD python WM_WS_M.py 127.0.0.1 0 5001 "$CENTRAL_IP" "$CENTRAL_PUERTO" "$WS_ID" "$UBICACION" & \
    sleep 1 && exec python WM_WS_E.py "$BROKER" 127.0.0.1 5001
