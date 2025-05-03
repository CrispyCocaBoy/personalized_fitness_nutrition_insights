## Gestione delle cartelle 
- Sensors -> Simula i dati che provengono dai vari sensori
- data_ingestion -> contiene tutti gli script che mandano i dati nella datalake
  - Architettura Medallion, per ogni cartella ci sta un relativo bucket nella datalake, cosi da gerarchizzare i dati 

## Docker 
- Dockerfile: contiene le specifiche per la creazione di una docker image
- docker-compose: Tutti i servizi (container), reti, volumi e configurazioni necessari per far girare il tuo sistema.
- per far girare il tutto usare mettere nel terminale "docker compose up --build" (build solamente la prima volta)

Importante: 
Per scaricare il database sql sul vostro progetto usate:
docker exec -t postgres_db pg_dump -U admin -d demographics --clean --if-exists --no-owner < database/demographics/dump.sql

Allo stesso modo per caricarlo (e poi metterlo su git usare)
docker exec -t postgres_db pg_dump -U admin -d demographics --clean --if-exists --no-owner > database/demographics/dump.sql



