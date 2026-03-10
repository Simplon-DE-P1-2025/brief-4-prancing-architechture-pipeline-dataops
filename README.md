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

## Configuration locale Soda

Le fichier `include/soda/configuration.yml` doit etre ajoute manuellement en local.

Il est ignore par Git et n'est pas versionne, car il contient les identifiants de connexion a PostgreSQL.

Exemple minimal:

```yaml
data_source chicago_crimes:
  type: postgres
  connection:
    host: postgres
    port: 5432
    username: postgres
    password: postgres
    database: chicago_crimes
```

Sans ce fichier, les taches `validate_raw_contract` et `validate_processed_contract` ne peuvent pas executer les contrats Soda.

## Regles de nettoyage

Le nettoyage technique applique par le pipeline est le suivant:
- lecture du brut extrait depuis l'API Socrata
- typage des colonnes numeriques et booleennes pendant la transformation
- conversion de `date` et `updated_on` en datetime
- normalisation des noms de colonnes en minuscules
- suppression des lignes sans `id`, `date` ou `primary_type` dans `clean_valid_raw`
- suppression des doublons restants sur `id` dans `clean_valid_raw`
- separation des lignes valides et des lignes en quarantaine apres `validate_raw_contract`
- production de rapports locaux dans `include/data/reports/`

## Regles metier

Les regles metier sont portees principalement par les contrats Soda.

Controle brut dans `raw_contract.yml`:
- volume minimal: `row_count > 1000`
- `id` obligatoire et unique
- `case_number` obligatoire et unique
- `date` obligatoire
- `primary_type` obligatoire
- `year >= 2001`
- `arrest` doit appartenir a `true`, `false`, `True`, `False`
- `domestic` doit appartenir a `true`, `false`, `True`, `False`

Controle apres transformation dans `processed_contract.yml`:
- volume minimal apres nettoyage: `row_count > 10000`
- `id` obligatoire et unique
- `case_number` obligatoire et unique
- `date` obligatoire
- `primary_type` obligatoire
- `year` obligatoire et `year >= 2001`
- `arrest` obligatoire
- `domestic` obligatoire
- `latitude` doit rester entre `41.6` et `42.1`
- `longitude` doit rester entre `-88.0` et `-87.5`

En sortie de chargement, les lignes hors bornes GPS sont redirigees vers la table et le CSV de quarantaine.

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

Puis ouvrir Airflow sur `http://localhost:8080` et lancer `dag_main` sur .

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
