import csv
import os

import requests
from airflow.sdk import BaseHook, Variable

from dags.chicago_pipeline.constants import FIELDNAMES, PAGE_SIZE, RAW_EXTRACTED_CSV_PATH


def fetch_and_save_csv(**_context):
    http_conn = BaseHook.get_connection("chicago_crimes_api")
    base_url = f"{http_conn.schema}://{http_conn.host}"
    api_limit = int(Variable.get("CHICAGO_API_LIMIT", default="20000"))
    endpoint = Variable.get("CHICAGO_API_ENDPOINT", default="/resource/ijzp-q8t2.json")
    url = f"{base_url}{endpoint}"

    offset = 0
    page = 1
    total_rows = 0

    print(f"Debut pagination - URL: {url}")
    print(f"PAGE_SIZE={PAGE_SIZE} | LIMIT total={api_limit}")

    os.makedirs(os.path.dirname(RAW_EXTRACTED_CSV_PATH), exist_ok=True)
    with open(RAW_EXTRACTED_CSV_PATH, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()

        with requests.Session() as session:
            while offset < api_limit:
                current_limit = min(PAGE_SIZE, api_limit - offset)
                params = {
                    "$limit": current_limit,
                    "$offset": offset,
                    "$order": "id ASC",
                }

                print(f"Page {page} - offset={offset} | limit={current_limit}")
                response = session.get(url, params=params, timeout=60)
                response.raise_for_status()
                page_data = response.json()

                if not page_data:
                    print(f"Fin pagination - page vide a l'offset {offset}")
                    break

                for record in page_data:
                    writer.writerow(record)

                batch_size = len(page_data)
                total_rows += batch_size
                print(f"  {batch_size} enregistrements recus | Total cumule: {total_rows}")

                if batch_size < current_limit:
                    print("Fin pagination - derniere page atteinte")
                    break

                offset += batch_size
                page += 1

    if total_rows == 0:
        raise ValueError("L'API a retourne 0 enregistrement - pipeline arrete")

    print(
        "Pagination terminee - "
        f"{total_rows} enregistrements ecrits dans {RAW_EXTRACTED_CSV_PATH}"
    )
