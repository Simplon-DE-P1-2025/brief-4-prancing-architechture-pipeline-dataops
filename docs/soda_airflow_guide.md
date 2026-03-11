# Guide : Soda & Apache Airflow

## Table des matières
1. [Présentation](#1-présentation)
2. [Installation de Soda](#2-installation-de-soda)
3. [Installation d'Airflow via Astro CLI](#3-installation-dairflow-via-astro-cli)
4. [Utilisation dans notre projet](#4-utilisation-dans-notre-projet)
5. [Autres manières d'utiliser Soda](#5-autres-manières-dutiliser-soda)
6. [Autres manières d'utiliser Airflow](#6-autres-manières-dutiliser-airflow)

---

## 1. Présentation

### Apache Airflow
Airflow est une plateforme d'orchestration de workflows de données. Il permet de :
- Définir des pipelines sous forme de **DAGs** (Directed Acyclic Graphs)
- Planifier et exécuter des tâches automatiquement
- Surveiller l'état des pipelines via une interface web

### Soda
Soda est un outil de **contrôle qualité des données**. Il permet de :
- Définir des règles de qualité en YAML (SodaCL)
- Exécuter des vérifications sur des tables en base de données
- Bloquer un pipeline si les données ne respectent pas les règles

---

## 2. Installation de Soda

### 2.1 Installation standard (pip)

```bash
# Connecteur PostgreSQL
pip install soda-core-postgres

# Autres connecteurs disponibles
pip install soda-core-bigquery      # Google BigQuery
pip install soda-core-snowflake     # Snowflake
pip install soda-core-spark         # Apache Spark
pip install soda-core-mysql         # MySQL
pip install soda-core-redshift      # Amazon Redshift
```

> **Note Python 3.12** : `distutils` a été supprimé en Python 3.12.
> Ajouter `setuptools` pour le fournir en fallback :
> ```bash
> pip install setuptools soda-core-postgres
> ```

### 2.2 Installation dans notre projet (via Astro CLI)

Dans `requirements.txt` :

```txt
# setuptools requis pour distutils (supprimé en Python 3.12)
setuptools
soda-core-postgres==3.5.6
```

Astro CLI installe automatiquement ces dépendances lors du build Docker :
```
astro dev start  →  docker build  →  pip install -r requirements.txt
```

---

## 3. Installation d'Airflow via Astro CLI

### 3.1 Installer Astro CLI

```bash
# macOS
brew install astro

# Linux
curl -sSL install.astronomer.io | sudo bash -s
```

### 3.2 Initialiser un projet

```bash
mkdir mon-projet-airflow
cd mon-projet-airflow
astro dev init
```

Crée la structure :
```
mon-projet-airflow/
├── dags/              ← vos DAGs Python
├── include/           ← fichiers supplémentaires
├── plugins/           ← plugins custom
├── tests/             ← tests des DAGs
├── Dockerfile         ← image Airflow
├── requirements.txt   ← dépendances Python
└── airflow_settings.yaml
```

### 3.3 Démarrer Airflow localement

```bash
astro dev start
```

Accès :
- **UI Airflow** : http://localhost:8080 (admin/admin)
- **PostgreSQL** : localhost:5432 (postgres/postgres)

### 3.4 Commandes utiles

```bash
astro dev restart          # Redémarrer (rebuild si requirements.txt modifié)
astro dev stop             # Arrêter
astro dev logs             # Voir les logs
astro dev run dags list    # Lister les DAGs
astro dev run dags trigger <dag_id>   # Déclencher un DAG
```

---

## 4. Utilisation dans notre projet

### 4.1 Structure des fichiers Soda

```
include/
└── soda/
    ├── soda_config.yml     ← connexion à PostgreSQL
    ├── checks_raw.yml      ← règles sur données brutes
    └── checks_clean.yml    ← règles sur données transformées
```

### 4.2 Configuration de la connexion (`soda_config.yml`)

```yaml
data_sources:
  chicago_crimes_db:
    type: postgres
    host: host.docker.internal   # depuis Docker → machine hôte
    port: 5432
    username: postgres
    password: postgres
    database: postgres
    schema: public
```

### 4.3 Définition des règles (`checks_raw.yml`)

```yaml
checks for chicago_crimes_raw:

  # Table non vide
  - row_count > 0

  # Champs obligatoires
  - missing_count(id) = 0
  - missing_count(primary_type) = 0
  - missing_count(date) = 0

  # Unicité
  - duplicate_count(id) = 0

  # Valeurs valides
  - invalid_count(arrest) = 0:
      valid values: ['true', 'false', 'True', 'False']
```

### 4.4 Définition des règles (`checks_clean.yml`)

```yaml
checks for chicago_crimes_clean:

  - row_count > 0
  - missing_count(crime_type) = 0
  - missing_count(year) = 0
  - missing_count(district) = 0
  - duplicate_count(id) = 0

  # Cohérence métier
  - min(year) >= 2000
  - max(year) <= 2025
  - min(hour) >= 0
  - max(hour) <= 23

  # Géolocalisation (bounding box Chicago)
  - min(latitude) >= 41.6
  - max(latitude) <= 42.1
  - min(longitude) >= -87.9
  - max(longitude) <= -87.5
```

### 4.5 Intégration dans les DAGs Airflow

```python
from soda.scan import Scan

def run_soda_check(checks_file: str, dataset: str) -> None:
    scan = Scan()
    scan.set_scan_definition_name(dataset)
    scan.set_data_source_name("chicago_crimes_db")
    scan.add_configuration_yaml_file(file_path="/path/to/soda_config.yml")
    scan.add_sodacl_yaml_file(file_path=checks_file)
    scan.execute()

    if scan.has_check_fails():
        raise ValueError(f"Qualité insuffisante :\n{scan.get_logs_text()}")

@task()
def soda_check_raw():
    run_soda_check("/path/to/checks_raw.yml", "chicago_crimes_raw")
```

### 4.6 Flux du pipeline avec Soda

```
Ingestion API
      ↓
load_raw_to_postgres
      ↓
soda_check_raw     ← GATE 1 : bloque si données brutes invalides
      ↓
transform_data
      ↓
load_clean_to_postgres
      ↓
soda_check_clean   ← GATE 2 : bloque si transformation incorrecte
      ↓
chicago_crimes_final (production)
```

---

## 5. Autres manières d'utiliser Soda

### 5.1 Soda CLI (ligne de commande)

Sans code Python, directement depuis le terminal :

```bash
# Installation
pip install soda-core-postgres

# Lancer un scan
soda scan -d chicago_crimes_db -c soda_config.yml checks_raw.yml

# Tester la connexion
soda test-connection -d chicago_crimes_db -c soda_config.yml
```

Idéal pour **tester les règles** avant de les intégrer dans Airflow.

### 5.2 Soda Cloud (SaaS)

Plateforme web disponible sur **cloud.soda.io** :
- Interface graphique pour créer et gérer les checks
- Dashboard de suivi de la qualité dans le temps
- Alertes email / Slack en cas d'échec
- Historique des scans
- Version gratuite disponible

Configuration pour envoyer les résultats vers Soda Cloud :

```yaml
# soda_config.yml
soda_cloud:
  host: cloud.soda.io
  api_key_id: <votre_api_key>
  api_key_secret: <votre_secret>
```

### 5.3 SodaScanOperator (opérateur Airflow dédié)

Intégration native avec Airflow, plus propre que notre approche manuelle :

```python
from soda.integrations.airflow import SodaScanOperator

check_raw = SodaScanOperator(
    task_id="soda_check_raw",
    data_sources={"chicago_crimes_db": postgres_hook},
    scan_definition_file="include/soda/checks_raw.yml",
    configuration_file="include/soda/soda_config.yml",
)
```

### 5.4 Comparaison des approches Soda

| Approche | Avantages | Inconvénients |
|----------|-----------|---------------|
| **pip + Scan()** (notre choix) | Simple, pas de dépendance extra | Code verbeux |
| **Soda CLI** | Rapide pour tester | Manuel, pas automatisé |
| **Soda Cloud** | UI, alertes, historique | Nécessite un compte |
| **SodaScanOperator** | Intégration native Airflow | Dépendance supplémentaire |

---

## 6. Autres manières d'utiliser Airflow

### 6.1 Installation directe (sans Astro CLI)

```bash
pip install apache-airflow
airflow db init
airflow webserver --port 8080
airflow scheduler
```

### 6.2 Via Docker Compose (officiel)

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
docker compose up -d
```

### 6.3 Sur un cluster Kubernetes (production)

Avec le chart Helm officiel :
```bash
helm repo add apache-airflow https://airflow.apache.org
helm install airflow apache-airflow/airflow
```

### 6.4 Astronomer Cloud (Astro CLI → Cloud)

Notre projet local peut être déployé sur Astronomer Cloud :
```bash
astro deploy          # Déployer le projet
astro deployment list # Voir les déploiements
```

### 6.5 Comparaison des approches Airflow

| Approche | Usage | Complexité |
|----------|-------|------------|
| **Astro CLI** (notre choix) | Développement local | Faible |
| **pip direct** | Tests simples | Faible |
| **Docker Compose** | Dev / staging | Moyenne |
| **Kubernetes + Helm** | Production | Élevée |
| **Astronomer Cloud** | Production managée | Faible (SaaS) |

---

## Ressources

- [Documentation Airflow](https://airflow.apache.org/docs/)
- [Documentation Astro CLI](https://www.astronomer.io/docs/astro/cli/overview)
- [Documentation Soda](https://docs.soda.io/)
- [SodaCL Reference](https://docs.soda.io/soda-cl/soda-cl-overview.html)
- [Soda + Airflow](https://docs.soda.io/soda/orchestrate-scans.html)
