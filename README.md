# Chicago Crimes Pipeline


Ce projet s'inscrit dans une demarche DataOps visant a industrialiser un pipeline de donnees automatise, robuste et reproductible. Il s'appuie sur Apache Airflow pour l'orchestration des workflows et sur Soda pour les controles de qualite, afin de mettre en place un flux fiable qui ingere les donnees, les valide, les transforme, effectue une seconde validation, puis les charge dans PostgreSQL.

L'objectif est de garantir l'automatisation, la fiabilite et la tracabilite du pipeline dans un contexte de deploiement local, cloud ou hybride, afin de reduire les erreurs manuelles, accelerer les livraisons et simplifier la maintenance.


## Lancement Airflow

```bash
astro dev start
```

Puis ouvrir Airflow sur `http://localhost:8080`.

Si tu modifies `requirements.txt` ou `Dockerfile`:

```bash
astro dev stop
astro dev start
```

## Lancement Streamlit avec Docker Compose

```bash
docker compose -f docker-compose.streamlit.yml up --build
```
## Lancement Streamlit en local (optionnel)

```bash
python3 -m venv .venv-streamlit
source .venv-streamlit/bin/activate
python -m pip install -r requirements-streamlit.txt
streamlit run streamlit_app.py
```

Commande pratique avec auto-detection du reseau Astro:

```bash
export ASTRO_AIRFLOW_NETWORK="$(docker inspect brief-4-prancing-architechture-pipeline-dataops_74776b-postgres-1 --format '{{range $k, $_ := .NetworkSettings.Networks}}{{$k}}{{end}}')"
docker compose -f docker-compose.streamlit.yml up --build
```

## Appel API et configuration Airflow

Le DAG `dag_main` recupere les donnees depuis l'API Socrata de Chicago via la connexion Airflow `chicago_crimes_api`.

Parametres utilises:
- connexion Airflow: `chicago_crimes_api`
- endpoint par defaut: `/resource/ijzp-q8t2.json`
- limite totale par defaut: `20000`
- taille de page de pagination dans le code: `1000`

Variables Airflow utilisees par `fetch_api_raw`:
- `CHICAGO_API_LIMIT`: nombre total maximal de lignes a recuperer
- `CHICAGO_API_ENDPOINT`: endpoint Socrata a appeler

Exemple d'appel effectue par le DAG:

```text
https://data.cityofchicago.org/resource/ijzp-q8t2.json?$limit=1000&$offset=0&$order=id ASC
```

Changer la limite de collecte:
1. Dans l'UI Airflow, aller dans `Admin > Variables`
2. Modifier `CHICAGO_API_LIMIT`
3. Relancer le DAG `dag_main`

Changer l'endpoint:
1. Dans l'UI Airflow, aller dans `Admin > Variables`
2. Modifier `CHICAGO_API_ENDPOINT`
3. Relancer le DAG `dag_main`

Configuration locale par defaut dans [airflow_settings.yaml](/home/dido/simplon_project/brief-4-prancing-architechture-pipeline-dataops/airflow_settings.yaml):
- `CHICAGO_API_LIMIT: 20000`
- `CHICAGO_API_ENDPOINT: /resource/ijzp-q8t2.json`

Si tu modifies [airflow_settings.yaml](/home/dido/simplon_project/brief-4-prancing-architechture-pipeline-dataops/airflow_settings.yaml), redemarre Astro pour reappliquer les variables et connexions:

```bash
astro dev stop
astro dev start
```

Point important:
- `CHICAGO_API_LIMIT` controle le volume total collecte
- `PAGE_SIZE=1000` est fixe dans [dag_main.py](/home/dido/simplon_project/brief-4-prancing-architechture-pipeline-dataops/dags/dag_main.py) et controle la pagination
- pour changer `PAGE_SIZE`, il faut modifier le code puis redemarrer Airflow
- si tu augmentes fortement la limite, le temps d'execution, les CSV generes et la volumetrie PostgreSQL augmenteront aussi

## Architecture fonctionnelle

```text
Chicago Open Data API
        |
        v
Airflow / Astro
  DAGs:
  - dag_main
  - dag_main_poc
        |
        +--> Ingestion
        |     - extraction API
        |     - validation Soda raw
        |     - separation valid / quarantine
        |
        +--> Transformation
        |     - nettoyage
        |     - cast des types
        |     - agregations
        |     - validation Soda processed
        |     - split processed valid / quarantine
        |
        +--> Loading
              - create_target_tables
              - load_valid_records
              - load_quarantine_records
        |
        v
PostgreSQL
  - chicago_crimes
  - chicago_crimes_quarantine
  - tables intermediaires de validation
        |
        v
Streamlit Dashboard
  - Overview
  - Soda Reports
  - Database Explorer
  - Quarantine Explorer
  - History
```

## Architecture technique

Technologies utilisees:
- `Astro CLI` pour le runtime local Airflow
- `Airflow 3 / Astro Runtime` pour l'orchestration
- `Soda` et `soda-postgres` pour les validations de qualite
- `PostgreSQL` comme base locale de travail et de restitution
- `pandas` pour les transformations et les split valid/quarantine
- `Streamlit` pour le dashboard local
- `Docker Compose` pour lancer Streamlit separement d'Astro

## DAGs

Le projet contient actuellement deux DAGs:
- [dag_main.py](/home/dido/simplon_project/brief-4-prancing-architechture-pipeline-dataops/dags/dag_main.py)
- [dag_main_poc.py](/home/dido/simplon_project/brief-4-prancing-architechture-pipeline-dataops/dags/dag_main_poc.py)

Le DAG de reference pour le projet est `dag_main`.

`dag_main_poc` est conserve uniquement comme POC pour tester ou comparer certaines fonctionnalites du DAG. Il ne constitue pas le flux principal documente dans ce README.

Flux principal de `dag_main`:
1. `fetch_api_raw`
2. `ensure_postgres_database`
3. `validate_raw_contract`
4. `publish_valid_raw` et `publish_quarantine_raw`
5. `clean_valid_raw` et `aggregate_valid_raw` en parallele
6. `finalize_clean_dataset`
7. `validate_processed_contract`
8. `split_processed_outputs`
9. `publish_valid_processed` et `publish_quarantine_processed`
10. `create_target_tables`
11. `load_valid_records` et `load_quarantine_records`

Schema du flux:

<img src="docs/images/schema_flux_airflow.png" alt="Schema du flux de dag_main" width="700" />


## Contrats Soda

Configuration source locale:
- `include/soda/configuration.yml`

Contrats:
- `include/soda/contracts/raw_contract.yml`
- `include/soda/contracts/processed_contract.yml`

Role:
- `validate_raw_contract` controle le brut extrait
- `validate_processed_contract` controle le dataset final avant chargement
- les rapports sont ecrits sous `include/data/reports/`

## Regles de nettoyage

Le pipeline applique notamment:
- lecture du brut extrait depuis l'API Socrata
- pagination `limit / offset` dans `dag_main`
- typage des colonnes numeriques et booleennes
- conversion de `date` et `updated_on` en `datetime`
- normalisation des noms de colonnes
- suppression des lignes sans `id`, `date` ou `primary_type`
- suppression des doublons restants sur `id`
- separation des lignes valides et des lignes en quarantaine

## Regles metier

Controles `raw`:
- `row_count > 1000`
- `id` obligatoire et unique
- `case_number` obligatoire et unique
- `date` obligatoire
- `primary_type` obligatoire
- `year >= 2001`
- `arrest` et `domestic` limites aux valeurs booleennes attendues

Controles `processed`:
- `row_count > 10000`
- `id` obligatoire et unique
- `case_number` obligatoire et unique
- `date` obligatoire
- `primary_type` obligatoire
- `year` obligatoire et `>= 2001`
- `arrest` obligatoire
- `domestic` obligatoire
- `latitude` entre `41.6` et `42.1`
- `longitude` entre `-88.0` et `-87.5`

## Base de donnees

Comportement actuel:
- la base `chicago_crimes` est creee seulement si elle n'existe pas
- les tables rechargees par le pipeline sont remplacees a chaque run
- le chargement n'est donc pas cumulatif dans l'etat actuel

Table finale valide:
- `public.chicago_crimes`

Table finale de quarantaine:
- `public.chicago_crimes_quarantine`

Tables intermediaires notables:
- `public.chicago_crimes_raw_contract`
- `public.chicago_crimes_raw_quarantine`
- `public.chicago_crimes_processed_contract`
- `public.chicago_crimes_processed_valid`
- `public.chicago_crimes_processed_quarantine`

## App Streamlit

Le dashboard local permet de consulter:
- les rapports Soda `raw` et `processed`
- la volumetrie des tables PostgreSQL
- les tables de quarantaine
- les apercus de tables en base
- un historique si des snapshots sont fournis

Structure:
- `streamlit_app.py`: page d'accueil
- `pages/1_Overview.py`
- `pages/2_Soda_Reports.py`
- `pages/3_Database_Explorer.py`
- `pages/4_Quarantine_Explorer.py`
- `pages/5_History.py`

Modules internes:
- `streamlit_dashboard/config.py`
- `streamlit_dashboard/services/reports.py`
- `streamlit_dashboard/services/postgres.py`
- `streamlit_dashboard/ui.py`

Fonctionnalites UX:
- bandeau de sante du pipeline
- cartes KPI
- visualisation de perte de volume
- filtres de quarantaine
- recherche texte
- bouton de rafraichissement
- telechargement CSV

## Secrets locaux

Le fichier `include/soda/configuration.yml` doit etre cree localement.

Il est ignore par Git et contient les identifiants PostgreSQL.

Exemple minimal:

```yaml
type: postgres
name: chicago_crimes

connection:
  host: postgres
  port: 5432
  user: postgres
  password: postgres
  database: chicago_crimes
```
## Fichiers generes

Le pipeline ecrit localement:
- `include/data/raw/*.csv`
- `include/data/processed/*.csv`
- `include/data/quarantine/*.csv`
- `include/data/reports/*.csv`
- `include/data/reports/*.md`

Ces fichiers sont ignores par Git.

## Arborescence du projet

```text
.
├── dags/
│   ├── dag_main.py
│   └── dag_main_poc.py
├── include/
│   ├── data/
│   │   ├── raw/
│   │   ├── processed/
│   │   ├── quarantine/
│   │   └── reports/
│   ├── soda/
│   │   ├── configuration.yml
│   │   └── contracts/
│   │       ├── raw_contract.yml
│   │       └── processed_contract.yml
│   └── sql/
│       └── init_tables.sql
├── pages/
│   ├── 1_Overview.py
│   ├── 2_Soda_Reports.py
│   ├── 3_Database_Explorer.py
│   ├── 4_Quarantine_Explorer.py
│   └── 5_History.py
├── streamlit_dashboard/
│   ├── config.py
│   ├── ui.py
│   └── services/
│       ├── postgres.py
│       └── reports.py
├── streamlit_app.py
├── Dockerfile
├── Dockerfile.streamlit
├── docker-compose.streamlit.yml
├── airflow_settings.yaml
├── requirements.txt
├── requirements-streamlit.txt
└── README.md
```
