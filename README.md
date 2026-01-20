# TP – Architecture Lakehouse Moderne
## Implémentation complète d'un Data Warehouse avec Delta Lake, dbt et Airflow

### Objectifs du Projet
- Moderniser une infrastructure analytique e-commerce pour du temps réel et du batch
- Ingestion de données brutes → couche Bronze (Delta Lake)
- Nettoyage, enrichissement, SCD Type 2 → couche Silver (Spark, Delta)
- Modélisation analytique (schéma étoile, faits/dimensions, métriques) → couche Gold
- Orchestration et automatisation des workflows avec Airflow
- Transformation et documentation modulaire avec dbt
- Gouvernance et qualité (Great Expectations, schema evolution, time travel)
- Ingestion et traitement en streaming avec Kafka + Spark Streaming

### Stack technologique
- **Delta Lake 3.0** (stockage ACID, gestion historique et évolution du schéma)
- **Apache Spark 3.5** (batch et streaming)
- **dbt (Data Build Tool)** (SQL analytics, tests, documentation, lineage)
- **Apache Airflow 2.8** (orchestration des pipelines complet)
- **Apache Kafka** (streaming en temps réel)
- **Great Expectations** (tests & qualité data)
- **Python 3.10+**

---

### Organisation du dépôt

- `data/` : échantillons de données brutes
- `spark/` : scripts PySpark batch et streaming (Bronze/Silver/Gold, SCD2…)
- `delta/` : scripts Delta Lake spécifiques (time travel, transactions, évolutions…)
- `kafka/` : scripts producteurs/consommateurs Kafka et configurations topic
- `dbt/` : projet dbt (modèles, seeds, tests…)
- `airflow/` : DAGs Python, guide exécution, configs Airflow (connexion et scheduling)
- `expectations/` : tests Great Expectations (profiling et validation data)
- `schema/` : diagrammes architecture, dataflow, médaillon…
- `bi/` : notebooks exploratoires, screenshots de dashboards analytiques
- `rapport.pdf` : rapport détaillé expliquant chaque étape, choix techniques, schémas

---

### Instructions d'exécution

1. Cloner ce dépôt et installer les dépendances (`pip install -r requirements.txt` ou `poetry install`)
2. Lancer/simuler Kafka pour l’ingestion en streaming (optionnel)
3. Exécuter les scripts d’ingestion et de transformation Spark pour constituer les layers Bronze, Silver, Gold
4. Déployer les modèles dbt pour modéliser et documenter les tables analytiques (Gold)
5. Lancer le DAG Airflow pour automatiser l’ensemble du pipeline
6. Valider la qualité des données avec Great Expectations (`expectations/`)
7. Explorer les dashboards et visualisations dans `bi/`
8. Pour l’analyse complète et les difficultés/solutions, voir `rapport.pdf`

---

### Membres du groupe

- Khadija Nachid Idrissi
- Rajae Fdili
- Aya Hamim
