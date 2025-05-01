## Gestione delle cartelle 
- Sensors -> Simula i dati che provengono dai vari sensori
- data_ingestion -> contiene tutti gli script che mandano i dati nella datalake
  - Architettura Medallion, per ogni cartella ci sta un relativo bucket nella datalake, cosi da gerarchizzare i dati 

## Docker 
- Dockerfile: contiene le specifiche per la creazione di una docker image
- docker-compose: Tutti i servizi (container), reti, volumi e configurazioni necessari per far girare il tuo sistema.
- per far girare il tutto 