# Pipeline DataOps — Chicago Crimes

Pipeline de données complet basé sur **Apache Airflow** (Astro CLI) pour ingérer, transformer, contrôler la qualité et charger les données de criminalité de la ville de Chicago (~8M enregistrements).

---

## Architecture du pipeline

```
┌─────────────────────────────────────────────────────────┐
│                     ORCHESTRATEUR                        │
│                                                          │
│   ┌─────────────────────────────────────────────────┐   │
│   │  TaskGroup : extraction                          │   │
│   │  ingest_data → load_raw_to_postgres → soda_raw  │   │
│   └──────────────────────┬──────────────────────────┘   │
│                           │                              │
│          ┌────────────────┴────────────────┐             │
│          ▼                                 ▼             │
│   ┌──────────────────┐   ┌──────────────────────────┐   │
│   │ TaskGroup :      │   │ TaskGroup : profiling     │   │
│   │ transformation   │   │ profiling_raw             │   │
│   │ transform_data   │   └──────────────┬────────────┘   │
│   │ load_clean_to_pg │                  │                │
│   │ soda_clean       │                  │                │
│   └──────────┬───────┘                  │                │
│              └──────────────┬───────────┘                │
│                             ▼                            │
│              ┌──────────────────────────┐                │
│              │ TaskGroup : chargement   │                │
│              │ load_final_to_postgres   │                │
│              └──────────────────────────┘                │
└─────────────────────────────────────────────────────────┘
```

---

## Stack technique

| Composant | Rôle |
|---|---|
| **Apache Airflow** (Astro CLI) | Orchestration du pipeline |
| **TaskFlow API + TaskGroup** | Définition des tâches et regroupement logique |
| **PostgreSQL** | Stockage des données brutes et transformées |
| **Soda Core** | Contrôle qualité des données |
| **API Chicago Data Portal** | Source de données ([ijzp-q8t2](https://data.cityofchicago.org/resource/ijzp-q8t2.json)) |
| **pandas** | Transformation des données |
| **ThreadPoolExecutor** | Ingestion parallèle (5 workers) |

---

## Structure du projet

```
.
├── dags/
│   ├── orchestrateur.py          # DAG principal avec TaskGroups
│   ├── extraction.py             # DAG extraction standalone
│   ├── transformation.py         # DAG transformation standalone
│   ├── chargement.py             # DAG chargement standalone
│   └── chicago_crimes_pipeline.py # Pipeline monolithique (référence)
├── include/
│   ├── pipeline_config.py        # Configuration partagée (URLs, chemins, colonnes)
│   ├── data/                     # Données CSV (ignorées par git)
│   └── soda/
│       ├── soda_config.yml       # Connexion Soda → PostgreSQL
│       ├── checks_raw.yml        # Checks qualité données brutes
│       └── checks_clean.yml      # Checks qualité données transformées
├── docs/
│   └── soda_airflow_guide.md     # Guide d'intégration Soda + Airflow
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Description des TaskGroups

### `extraction` (3 tâches)
1. **`ingest_data`** — Récupère les données depuis l'API Chicago en parallèle (5 workers, lots de 20 000 lignes) avec retry automatique en cas de timeout
2. **`load_raw_to_postgres`** — Charge le CSV brut dans la table `chicago_crimes_raw` via bulk insert
3. **`soda_check_raw`** — Vérifie la qualité des données brutes (nulls, unicité des IDs, valeurs arrest valides)

### `transformation` (3 tâches) — parallèle avec profiling
1. **`transform_data`** — Nettoyage, typage, filtrage (années 2000-2025), extraction heure depuis datetime
2. **`load_clean_to_postgres`** — Charge les données transformées dans `chicago_crimes_clean`
3. **`soda_check_clean`** — Vérifie la qualité des données transformées (coordonnées géographiques, heures valides, pas de doublons)

### `profiling` (1 tâche) — parallèle avec transformation
- **`profiling_raw`** — Génère des statistiques sur les données brutes (total lignes, nulls, top 5 types de crimes)

### `chargement` (1 tâche)
- **`load_final_to_postgres`** — Renomme `chicago_crimes_clean` en `chicago_crimes_final` (table de production)

---

## Contrôles qualité Soda

### Sur les données brutes (`checks_raw.yml`)
- Table non vide
- Champs obligatoires non nuls : `id`, `primary_type`, `date`
- Unicité des IDs
- Valeurs valides pour `arrest` : `true`, `false`, `True`, `False`

### Sur les données transformées (`checks_clean.yml`)
- Table non vide
- Champs clés non nuls : `crime_type`, `year`, `district`
- Années cohérentes : 2000 ≤ year ≤ 2025
- Pas de doublons sur `id`
- Heures valides : 0 ≤ hour ≤ 23
- Coordonnées dans la bounding box de Chicago :
  - Latitude : 41.6 — 42.1
  - Longitude : -87.9 — -87.5

---

## Schéma des tables PostgreSQL

### `chicago_crimes_raw`
| Colonne | Type |
|---|---|
| id | TEXT |
| date | TEXT |
| primary_type | TEXT |
| description | TEXT |
| location_description | TEXT |
| arrest | TEXT |
| domestic | TEXT |
| district | TEXT |
| ward | TEXT |
| community_area | TEXT |
| year | TEXT |
| latitude | TEXT |
| longitude | TEXT |

### `chicago_crimes_final`
| Colonne | Type |
|---|---|
| id | TEXT |
| date | DATE |
| hour | SMALLINT |
| crime_type | TEXT |
| description | TEXT |
| location_description | TEXT |
| arrest | BOOLEAN |
| domestic | TEXT |
| district | TEXT |
| year | INTEGER |
| latitude | FLOAT |
| longitude | FLOAT |

---

## Installation et lancement

### Prérequis
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)
- Docker Desktop

### Démarrage

```bash
# Cloner le repo
git clone https://github.com/MariamDouamba/brief-4-prancing-architechture-pipeline-dataops.git
cd brief-4-prancing-architechture-pipeline-dataops

# Démarrer Airflow
astro dev start

# Accéder à l'UI
open http://localhost:8080
# Login : admin / admin
```

### Connexion PostgreSQL (DBeaver / psql)
```
Host     : localhost
Port     : 5432
Database : postgres
User     : postgres
Password : postgres
```

### Lancer le pipeline
1. Dans l'UI Airflow, activer le DAG **`orchestrateur`**
2. Cliquer sur **Déclencher**
3. Durée estimée : ~20-30 min pour 2M lignes

---

## Configuration

Tous les paramètres sont centralisés dans [include/pipeline_config.py](include/pipeline_config.py) :

```python
MAX_ROWS = 2000000      # Nombre de lignes à ingérer (None = tout ~8M)
API_BATCH_SIZE = 20000  # Taille des lots API
WINDOW = 5              # Nombre de workers parallèles
```

---

## Choix d'architecture : TaskGroup vs DAGs séparés

Ce projet utilise une approche **DAG unifié avec TaskGroups** plutôt que plusieurs DAGs reliés par `TriggerDagRunOperator` :

| | TaskGroup (choix retenu) | DAGs séparés |
|---|---|---|
| Lisibilité UI | Groupes collapsables | Logs éparpillés |
| Performance | Pas de latence inter-DAGs | Polling `poke_interval` |
| Factorisation | `run_soda_check` défini une fois | Code dupliqué |
| Flexibilité | Re-run par groupe possible | Chaque DAG indépendant |

Les DAGs standalone (`extraction.py`, `transformation.py`, `chargement.py`) sont conservés pour permettre une exécution indépendante de chaque étape si besoin.
