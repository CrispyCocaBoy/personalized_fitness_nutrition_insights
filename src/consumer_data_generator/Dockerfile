FROM python:3.11-slim

# Crea directory di lavoro
WORKDIR /app

# Installa dipendenze di sistema per bcrypt e psycopg2
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*


COPY src/consumer_data_generator .
COPY utility ./utility

# Installa pacchetti Python
RUN pip install psycopg2-binary bcrypt faker

# Comando per eseguire lo script
CMD ["python", "fake_user.py"]
