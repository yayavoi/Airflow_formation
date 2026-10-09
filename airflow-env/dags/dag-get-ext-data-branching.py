from datetime import datetime, timedelta
import random

import pandas as pd

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)


# Arguments par défaut
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


# Traitement des données
def process_data():
    input_file = "/opt/airflow/shared-data/flowerdataset.csv"
    output_file = "/opt/airflow/shared-data/flowerdataset_processed.csv"

    df = pd.read_csv(input_file)

    df["volume"] = df["sepal_length"] * df["sepal_width"]

    df.to_csv(output_file, index=False)

    print("Données traitées avec succès !")
    print(df.head())


# Fonction de branchement aléatoire
def choose_branch():
    if random.choice([True, False]):
        return "ramasser_fleurs"
    else:
        return "travailler"


# Définition du DAG
with DAG(
    dag_id="dag-get-ext-data",
    description="Téléchargement, traitement et branchement conditionnel",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["flowers", "branching"],
) as dag:

    # Étape 1 : télécharger les données
    download_data = BashOperator(
        task_id="download_data",
        bash_command=(
            "mkdir -p /opt/airflow/shared-data && "
            "curl --fail --location --silent --show-error "
            "https://raw.githubusercontent.com/CourseMaterial/"
            "DataWrangling/main/flowerdataset.csv "
            "-o /opt/airflow/shared-data/flowerdataset.csv"
        ),
    )

    # Étape 2 : traiter les données
    process_data_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    # Étape 3 : choisir aléatoirement une branche
    choose_branch_task = BranchPythonOperator(
        task_id="choose_branch",
        python_callable=choose_branch,
    )

    # Branche A : journée favorable
    ramasser_fleurs = BashOperator(
        task_id="ramasser_fleurs",
        bash_command="echo 'Belle journee pour ramasser des fleurs'",
    )

    # Branche B : journée défavorable
    travailler = BashOperator(
        task_id="travailler",
        bash_command="echo 'Malheureusement il faut travailler'",
    )

    # Définir les dépendances
    download_data >> process_data_task >> choose_branch_task

    choose_branch_task >> [ramasser_fleurs, travailler]

