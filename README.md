# brief-4-prancing-architechture-pipeline-dataops
# 🚔 Chicago Crimes Pipeline — Airflow + Soda + PostgreSQL

Pipeline de données automatisé qui ingère les crimes de Chicago depuis une API publique, valide la qualité avec Soda, transforme les données et les charge dans PostgreSQL — orchestré avec Airflow via **Astro CLI**.

---

## 🏗️ Architecture du pipeline

```
API Chicago Crimes (JSON)
        ↓
[Task 1] fetch_data          → Ingestion des données brutes
        ↓
[Task 2] validate_raw        → Contrôle qualité Soda (données brutes)
        ↓
[Task 3] transform_data      → Filtrage, agrégation, restructuration
        ↓
[Task 4] validate_processed  → Contrôle qualité Soda (données transformées)
        ↓
[Task 5] load_to_postgres    → Chargement final dans PostgreSQL
```

---

## 📋 Prérequis

- Windows 10/11 avec **WSL2** activé (Ubuntu)
- **Docker Desktop** installé et configuré avec WSL2
- **VSCode** avec l'extension **WSL** (Microsoft)
- Git

---

## 🐳 Étape 1 — Docker Desktop

1. Télécharge et installe Docker Desktop : https://www.docker.com/products/docker-desktop

2. Dans Docker Desktop :
   - Ouvre **Settings → Resources → WSL Integration**
   - Active ta distro **Ubuntu** ✅
   - Clique **Apply & Restart**

3. Vérifie dans ton terminal WSL :
```bash
docker --version
docker compose version
```

---

## 🚀 Étape 2 — Installer Astro CLI

Dans ton terminal **WSL (Ubuntu)** :

```bash
# Télécharger et installer Astro CLI
curl -sSL install.astronomer.io | sudo bash

# Vérifier l'installation
astro version
```

---

## 📁 Étape 3 — Cloner et initialiser le projet

```bash
# Cloner le repo
git clone https://github.com/ton-username/ton-repo.git
cd ton-repo

# Initialiser la structure Astro
astro dev init
```

Astro génère automatiquement cette structure :

```
ton-repo/
├── dags/               ← tes DAGs Airflow
├── include/            ← fichiers additionnels (soda, data...)
├── plugins/            ← plugins Airflow custom
├── tests/              ← tests des DAGs
├── Dockerfile          ← image Airflow custom
├── packages.txt        ← dépendances système (apt)
├── requirements.txt    ← dépendances Python
└── .astro/
    └── config.yaml
```

---

## 📂 Étape 4 — Créer la structure Soda

```bash
# Créer les dossiers nécessaires
mkdir -p include/soda/checks
mkdir -p include/data/{raw,processed}
```

Structure finale du projet :

```
ton-repo/
├── dags/
│   ├── chicago_crimes_dag.py       ← DAG principal
│   └── utils/
│       ├── api_client.py           ← Client API Chicago
│       └── soda_runner.py          ← Wrapper Soda
├── include/
│   ├── soda/
│   │   ├── configuration.yml       ← Connexion PostgreSQL Soda
│   │   └── checks/
│   │       ├── raw_data_checks.yml
│   │       └── transformed_checks.yml
│   └── data/
│       ├── raw/
│       └── processed/
├── Dockerfile
├── requirements.txt
├── packages.txt
└── .env
```

---

## 📦 Étape 5 — Configurer les dépendances

### `requirements.txt`

```txt
soda-core-postgres==3.3.3
apache-airflow-providers-postgres==5.10.0
apache-airflow-providers-http==4.10.0
requests==2.31.0
pandas==2.2.2
python-dotenv==1.0.0
```

### `.env` (copier depuis `.env.example`)

```bash
cp .env.example .env
```

Remplis les valeurs dans `.env` :

```env
# API Chicago Crimes
API_URL=https://data.cityofchicago.org/resource/ijzp-q8t2.json
API_LIMIT=20000
SOCRATA_APP_TOKEN=        # optionnel, évite le rate limit

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=chicago_crimes_db
POSTGRES_USER=ton_user
POSTGRES_PASSWORD=ton_password
```

> 💡 **Token Socrata gratuit** (recommandé) : https://data.cityofchicago.org/profile/app_tokens

---

## ▶️ Étape 6 — Lancer le projet

```bash
astro dev start
```

> ⏳ Docker démarre automatiquement : webserver + scheduler + triggerer + PostgreSQL

---

## 🌐 Étape 7 — Accéder à l'interface Airflow

Ouvre ton navigateur Windows : **http://localhost:8080**

| Champ    | Valeur  |
|----------|---------|
| Login    | `admin` |
| Password | `admin` |

Active le DAG `chicago_crimes_pipeline` et lance-le manuellement ✅

---

## 🔌 Configurer la connexion PostgreSQL dans Airflow

Dans l'UI Airflow → **Admin → Connections → +** :

| Champ    | Valeur              |
|----------|---------------------|
| Conn Id  | `postgres_default`  |
| Type     | `Postgres`          |
| Host     | `postgres`          |
| Schema   | `chicago_crimes_db` |
| Login    | `ton_user`          |
| Password | `ton_password`      |
| Port     | `5432`              |

---

## 🛑 Commandes Astro CLI

```bash
astro dev start      # Démarrer tous les services
astro dev stop       # Arrêter tous les services
astro dev restart    # Redémarrer après modif Dockerfile/requirements
astro dev logs       # Voir les logs en temps réel
astro dev ps         # Voir les containers actifs
```

---

## 🩺 Dépannage

**Docker ne démarre pas dans WSL**
```bash
docker ps   # si erreur → relancer Docker Desktop sur Windows
```

**Port 8080 déjà utilisé**
```bash
astro dev start --port 8081
```

**Mot de passe Linux oublié**
```powershell
# PowerShell Windows (admin)
wsl -u root
passwd ton_username
exit
```

---

## 📄 Licence

Voir [LICENSE](./LICENSE)
