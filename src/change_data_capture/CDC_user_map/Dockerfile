FROM python:3.11-slim

WORKDIR /app

# Installo librerie
COPY CDC_user_map.py /app/
RUN pip install --no-cache-dir confluent-kafka redis pyspark

CMD ["python", "CDC_user_map.py"]