FROM python:3.11-slim

WORKDIR /app

# Installa tutte le librerie Python necessarie in un unico comando per ottimizzare i layer
RUN pip install streamlit psycopg2-binary bcrypt pycountry

# Copia l'intero contenuto della cartella del progetto nel container
COPY . .

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
