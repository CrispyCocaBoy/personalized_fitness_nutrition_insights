FROM python:3.11

# Crea directory di lavoro
WORKDIR /app

# Installa dipendenze di sistema per bcrypt e psycopg2
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*


COPY src/data_generator/producer_user_generator .
COPY utility ./utility

# Installa pacchetti Python
RUN pip install "psycopg[binary]" bcrypt faker

# Comando per eseguire lo script
CMD ["python", "fake_user.py"]
