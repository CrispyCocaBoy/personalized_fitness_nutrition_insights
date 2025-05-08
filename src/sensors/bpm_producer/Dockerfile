FROM python:3.11
WORKDIR /app
COPY . .
RUN pip install paho-mqtt
CMD ["python", "bpm_producer_test.py"]