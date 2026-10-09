from datetime import datetime, timedelta

import pandas as pd

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


# Arguments par défaut
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 4,
    "retry_delay": timedelta(minutes=1),
}


# Fonction Python de traitement des données
def process_data():
    # Chemins des fichiers
    input_file = "/tmp/flowerdataset.csv"
    output_file = "/tmp/flowerdataset_processed.csv"

    # Lire le fichier CSV
    df = pd.read_csv(input_file)

    # Vérifier la présence des colonnes nécessaires
    required_columns = ["sepal_length", "sepal_width"]

    for column in required_columns:
        if column not in df.columns:
            raise ValueError(
                f"La colonne obligatoire '{column}' est absente du CSV."
            )

    # Calculer la colonne volume
    df["volume"] = df["sepal_length"] * df["sepal_width"]

    # Enregistrer le résultat
    df.to_csv(output_file, index=False)

    # Afficher un aperçu dans les logs Airflow
    print(df.head())

    print(f"Fichier traité enregistré dans : {output_file}")


# Définition du DAG
with DAG(
    dag_id="flower_data_processing",
    description="Téléchargement et traitement du jeu de données des fleurs",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["csv", "python", "bash"],
) as dag:

    # Tâche 1 : téléchargement du fichier CSV
    download_data = BashOperator(
        task_id="download_data",
        bash_command=(
            "curl --fail --location --silent --show-error "
            "https://raw.githubusercontent.com/CourseMaterial/"
            "DataWrangling/main/flowerdataset.csv "
            "-o /tmp/flowerdataset.csv"
        ),
    )

    # Tâche 2 : traitement du fichier CSV
    process_data_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    # Définir l'ordre d'exécution
    download_data >> process_data_task
