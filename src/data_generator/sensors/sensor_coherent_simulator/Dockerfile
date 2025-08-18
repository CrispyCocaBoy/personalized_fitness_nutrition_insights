FROM python:3.11
WORKDIR /app


COPY src/data_generator/sensors/sensor_coherent_simulator/ .
COPY utility ./utility

RUN pip install paho-mqtt psycopg[binary] bcrypt boto3 pytz redis pandas
CMD ["python", "sensor_coherent_simulator.py"]