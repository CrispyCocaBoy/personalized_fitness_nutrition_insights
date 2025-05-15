FROM python:3.11
WORKDIR /app

# Installa kafka-python
RUN pip install  kafka-python

# Copia lo script di creazione del topic
COPY src/kafka/ .

# Comando di avvio
CMD ["python", "create_kafka_topic.py"]


