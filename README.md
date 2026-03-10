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

Flux principal:
1. `fetch_and_save_csv`
2. `validate_raw` avec contrat Soda
3. `valid_raw` et `quarantine_raw`
4. `transform_filter` et `transform_agg` en parallele
5. `merge_and_finalize`
6. `validate_processed` avec contrat Soda
7. `create_tables_if_not_exists`
8. `load_valid_data` et `load_quarantine_data` en parallele

## Contrats Soda

Configuration source:
- `include/soda/configuration.yml`

Contrats:
- `include/soda/contracts/raw_contract.yml`
- `include/soda/contracts/processed_contract.yml`

`validate_raw`:
- execute le contrat Soda sur le brut extrait
- produit ensuite un fichier valide et un fichier de quarantaine
- ecrit des rapports CSV dans `include/data/reports/`

`validate_processed`:
- execute le contrat Soda sur le dataset propre
- ecrit aussi des rapports CSV dans `include/data/reports/`
- bloque le chargement final si le contrat echoue

## Fichiers generes

Le pipeline genere localement:
- `include/data/raw/*.csv`
- `include/data/processed/*.csv`
- `include/data/quarantine/*.csv`
- `include/data/reports/*.csv`

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
