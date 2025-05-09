FROM python:3.11
WORKDIR /app
COPY . .
RUN pip install paho-mqtt psycopg2 bcrypt
CMD ["python", "bpm_producer_test.py"]