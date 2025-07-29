FROM python:3.11
WORKDIR /app


COPY src/data_generator/sensors/people_simulator/ .
COPY utility ./utility

RUN pip install paho-mqtt psycopg[binary] bcrypt boto3
CMD ["python", "people_simulator.py"]
