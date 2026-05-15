# Plateforme Big Data de Traitement et d’Analyse des News

Projet complet de pipeline Big Data permettant la collecte, le streaming, le traitement, le stockage, l’orchestration, le monitoring et la visualisation de données provenant de sites d’actualités.

Le projet utilise plusieurs technologies modernes telles que Kafka, Spark, Airflow, MinIO, PostgreSQL, Metabase, Prometheus et Grafana.

Structure Docker et configuration fournies dans le projet utilisateur. :contentReference[oaicite:0]{index=0}

---

# Technologies Utilisées

- Apache Kafka
- Apache Spark
- Apache Airflow
- PostgreSQL
- MinIO
- Metabase
- Prometheus
- Grafana
- Docker & Docker Compose
- Python

---

# Architecture du Projet

Le pipeline suit une architecture Big Data moderne :

1. Les articles sont récupérés depuis :
   - CNN
   - Al Jazeera

2. Les données sont envoyées en streaming via Kafka.

3. Spark Streaming consomme les données Kafka et effectue les traitements.

4. Les données sont stockées dans plusieurs couches :
   - Bronze Layer → données brutes
   - Silver Layer → données nettoyées
   - Gold Layer → données analytiques finales

5. Airflow orchestre les workflows ETL.

6. PostgreSQL stocke les données du Data Warehouse.

7. Metabase fournit les dashboards analytiques.

8. Prometheus collecte les métriques.

9. Grafana affiche les dashboards de monitoring.

---

# Structure du Projet

```bash
bigdata-stack/
│
├── aljazeera.py
├── cnn.py
├── docker-compose.yml
├── Dockerfile.airflow
├── prometheus.yml
├── requirements.txt
│
├── dags/
│   ├── aljazeera_dag.py
│   └── gold_layer_dag.py
│
├── init/
│   ├── init-metabase.sql
│   ├── init_metabase.py
│   │
│   ├── dashboards/
│   │   ├── dashboard_config.yml
│   │   └── main_dashboard.json
│   │
│   └── datasources/
│       └── datasource.yml
│
└── spark_jobs/
    ├── gold_processor.py
    ├── spark_processor.py
    └── verify_silver.py
```

---

# Couches de Données (Data Layers)

## Bronze Layer

Contient les données brutes récupérées directement depuis les sites d’actualités.

Stockage :
- MinIO
- Bucket : `bronze`

---

## Silver Layer

Contient les données nettoyées et transformées après traitement Spark.

Stockage :
- MinIO
- Bucket : `silver`

---

## Gold Layer

Contient les données finales utilisées pour :
- les dashboards
- les statistiques
- les analyses métiers

Stockage :
- PostgreSQL (warehouse)

---

# Services Inclus

| Service | Description |
|---|---|
| PostgreSQL | Base de données principale |
| Kafka | Streaming temps réel |
| Zookeeper | Coordination Kafka |
| Spark Master | Gestion du cluster Spark |
| Spark Worker | Exécution des traitements Spark |
| Airflow | Orchestration ETL |
| MinIO | Stockage objet |
| Metabase | Dashboard analytique |
| Prometheus | Collecte des métriques |
| Grafana | Visualisation et monitoring |

---

# Lancement du Projet
## Prérequis

Avant de lancer le projet, assurez-vous d’avoir les éléments suivants installés :

- Docker
- Docker Compose
- Python 3.11

Le projet nécessite obligatoirement Python 3.11 pour assurer la compatibilité des dépendances et des services.

Vérification de la version Python :

```bash
python --version
```

La sortie doit être similaire à :

```bash
Python 3.11.x
```

## 1. Build des conteneurs

```bash
docker compose build
```

---

## 2. Démarrage des services

```bash
docker compose up -d
```

---

## 3. Vérification des conteneurs

```bash
docker ps
```

---

# Accès aux Interfaces

# Airflow

URL :

```text
http://localhost:8088/
```

Identifiants :

```text
Username : admin
Password : admin123
```

Airflow permet :
- d’exécuter les DAGs
- d’orchestrer les workflows
- de surveiller les tâches ETL

---

# Metabase

URL :

```text
http://localhost:3000/
```

Identifiants :

```text
Email : admin@admin.com
Password : AdminPassword123!
```

Metabase est configuré automatiquement grâce au script :

```text
init/init_metabase.py
```

Le script :
- attend le démarrage de Metabase
- crée le compte administrateur
- connecte automatiquement PostgreSQL

---

# MinIO Console

URL :

```text
http://localhost:9001/
```

Identifiants :

```text
Username : admin
Password : admin123
```

Buckets créés automatiquement :
- bronze
- silver

MinIO sert de Data Lake pour le projet.

---

# Grafana

URL :

```text
http://localhost:3001/
```

Identifiants :

```text
Username : admin
Password : admin
```

Grafana permet :
- le monitoring du cluster Spark
- le suivi des DAGs Airflow
- l’analyse des métriques système

---

# Prometheus

URL :

```text
http://localhost:9090/
```

Prometheus collecte les métriques depuis :
- Spark
- Airflow
- StatsD Exporter

---

# Spark Master UI

URL :

```text
http://localhost:8080/
```

Permet de surveiller :
- les applications Spark
- les workers
- les jobs actifs
- les executors

---

# Spark Worker UI

URL :

```text
http://localhost:8081/
```

Permet de surveiller :
- l’utilisation CPU/RAM
- les tâches Spark en cours

---

# DAGs Airflow

## aljazeera_dag.py

Responsable de :
- lancer le scraping Al Jazeera
- gérer l’ingestion des données

---

## gold_layer_dag.py

Responsable de :
- traiter les données Silver
- générer les données Gold analytiques

---

# Pipeline Streaming

## CNN Streamer

Conteneur :

```text
cnn-streamer
```

Responsabilités :
- scraper les articles CNN
- envoyer les données vers Kafka

---

## Spark Streaming Job

Conteneur :

```text
spark-streaming-job
```

Responsabilités :
- consommer les données Kafka
- traiter les flux temps réel
- sauvegarder les données dans MinIO (Bronze et Silver)

---

# Monitoring

## Prometheus

Collecte :
- métriques Spark
- métriques Airflow
- métriques système

---

## Grafana

Affiche :
- monitoring Spark
- monitoring Airflow
- activité streaming
- performances ETL

---

# Configuration PostgreSQL

Identifiants par défaut :

```text
User : admin
Password : admin123
```

Bases utilisées :
- airflow
- metabase
- warehouse

---

# Initialisation Automatique de Metabase

Le fichier :

```text
init/init_metabase.py
```

Permet automatiquement :
- d’attendre le démarrage de Metabase
- de créer le compte administrateur
- d’ajouter la base PostgreSQL warehouse
- de configurer l’environnement BI

---

# Configuration Kafka

Ports Kafka :

| Port | Usage |
|---|---|
| 9092 | Communication Docker interne |
| 29092 | Accès localhost |

---

# Commandes Docker Utiles

## Arrêter les services

```bash
docker compose down
```

---

## Redémarrer les services

```bash
docker compose restart
```

---

## Voir les logs

```bash
docker compose logs -f
```

---

## Rebuild complet

```bash
docker compose up --build
```

---

# Fonctionnalités du Projet

- Scraping automatique d’articles
- Streaming temps réel avec Kafka
- Traitement Big Data avec Spark
- Orchestration ETL avec Airflow
- Data Lake avec MinIO
- Data Warehouse PostgreSQL
- Dashboards analytiques avec Metabase
- Monitoring avec Prometheus et Grafana

---

# Réseau Docker

Tous les services communiquent via :

```text
bigdata-net
```

---

# Stockage des Données

Les données persistantes sont stockées dans :

```text
./data/
```

---

# Auteur

Projet Big Data — Plateforme de Traitement et d’Analyse des News

Technologies :
Docker, Kafka, Spark, Airflow, PostgreSQL, MinIO, Metabase, Prometheus et Grafana.
