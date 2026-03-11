from airflow.decorators import dag, task
from pendulum import datetime
#import json
import csv
import requests
import io

@dag(
    schedule="@daily",  
    start_date=datetime(2026, 3, 10),
    catchup=False,
    tags=["etl"]
)
def extract_chicago():
  
    @task()
    def fetch_data():
        url = "https://data.cityofchicago.org/resource/ijzp-q8t2.json?$limit=20000"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()  
            print(f"Nombre d'enregistrements récupérés : {len(data)}")
            return data
        else:
            raise Exception(f"Erreur HTTP {response.status_code}")

    @task()
    def json_to_csv(data):
        if not data:
            return ""

        keys = data[0].keys()
        output = io.StringIO()
        dict_writer = csv.DictWriter(output, fieldnames=keys, delimiter=',')
        dict_writer.writeheader()
        dict_writer.writerows(data)

        csv_content = output.getvalue()
        output.close()

        print(f"Taille du CSV généré : {len(csv_content)} caractères")
        return csv_content

    data = fetch_data()
    csv_data = json_to_csv(data)

dag_instance = extract_chicago()



