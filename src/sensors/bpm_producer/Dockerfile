FROM python:3.11
WORKDIR /app

# Copia solo il codice del producer
COPY src/sensors/bpm_producer/ .

# Copia il pacchetto utility nella stessa cartella
COPY utility ./utility

RUN pip install paho-mqtt psycopg2 bcrypt
CMD ["python", "bpm_producer_test.py"]
