FROM python:3.11-slim

WORKDIR /app

# Installo librerie
COPY change_data_capture.py /app/
RUN pip install --no-cache-dir confluent-kafka redis

CMD ["python", "change_data_capture.py"]