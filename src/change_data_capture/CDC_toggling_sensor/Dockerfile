FROM python:3.11-slim

WORKDIR /app

# Installo librerie
COPY CDC_toggling_sensor.py /app/
RUN pip install --no-cache-dir confluent-kafka redis

CMD ["python", "CDC_toggling_sensor.py"]