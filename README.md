# Chicago Crimes Pipeline

Pipeline Airflow lance via Astro CLI pour ingerer des crimes de Chicago, controler la qualite avec Soda, transformer les donnees et les charger dans PostgreSQL.

## Arborescence utile

```text
.
├── dags/
│   └── dag_main.py
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
├── airflow_settings.yaml
├── Dockerfile
├── packages.txt
└── requirements.txt
```

## DAG

Le projet expose un seul DAG: `dag_main`.

Il contient trois `TaskGroup`:
- `ingestion`
- `transformation`
- `loading`

Chaque groupe est decoupe en sous-groupes legers pour une lecture plus claire dans l'UI Airflow:
- `ingestion.extract`
- `ingestion.quality`
- `ingestion.outputs`
- `transformation.prepare`
- `transformation.quality`
- `loading.init`
- `loading.publish`

Flux principal:
1. `fetch_api_raw`
2. `ensure_postgres_database`
3. `validate_raw_contract`
4. `publish_valid_raw` et `publish_quarantine_raw`
5. `clean_valid_raw` et `aggregate_valid_raw` en parallele
6. `finalize_clean_dataset`
7. `validate_processed_contract`
8. `create_target_tables`
9. `load_valid_records` et `load_quarantine_records` en parallele

## Contrats Soda

Configuration source:
- `include/soda/configuration.yml`

Contrats:
- `include/soda/contracts/raw_contract.yml`
- `include/soda/contracts/processed_contract.yml`

`validate_raw`:
- execute le contrat Soda sur le brut extrait
- produit ensuite un fichier valide et un fichier de quarantaine
- ecrit des rapports CSV et Markdown dans `include/data/reports/`

`validate_processed`:
- execute le contrat Soda sur le dataset propre
- ecrit aussi des rapports CSV et Markdown dans `include/data/reports/`
- bloque le chargement final si le contrat echoue

## Fichiers generes

Le pipeline genere localement:
- `include/data/raw/*.csv`
- `include/data/processed/*.csv`
- `include/data/quarantine/*.csv`
- `include/data/reports/*.csv`
- `include/data/reports/*.md`

Ces fichiers sont ignores par Git.

## Dependances

Le projet utilise notamment:
- `apache-airflow-providers-postgres`
- `apache-airflow-providers-http`
- `soda`
- `soda-postgres`
- `psycopg2-binary`
- `pandas`

## Lancement

```bash
astro dev start
```

Puis ouvrir Airflow sur `http://localhost:8080` et lancer `dag_main`.

Si tu modifies `requirements.txt` ou `Dockerfile`:

```bash
astro dev stop
astro dev start
```

## Configuration locale

Le bootstrap local passe par `airflow_settings.yaml`.

Connexions attendues:
- `chicago_crimes_api`
- `postgres_default`
- `postgres_root`

Variables attendues:
- `CHICAGO_API_LIMIT`
- `CHICAGO_API_ENDPOINT`
