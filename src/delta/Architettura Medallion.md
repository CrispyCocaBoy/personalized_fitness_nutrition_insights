# Architettura Medallion

## 📌 Cos'è l'architettura Medallion?
L'architettura **Medallion** è una strategia per organizzare i dati all'interno di un **Data Lake**, suddividendoli in tre livelli progressivi di qualità:

- **Bronze**: dati grezzi, appena acquisiti, senza pulizia o trasformazioni.
- **Silver**: dati puliti, validati e arricchiti con alcune elaborazioni di base.
- **Gold**: dati pronti per l'analisi, aggregati e ottimizzati per insight, dashboard o modelli.

> L'idea è: **non sovrascrivere mai i dati originali**, ma trasformarli a step e conservarne ogni versione.
