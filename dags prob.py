from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import re

default_args = {
    "depends_on_pas": False,
    "start_date": datetime(2022, 1, 1),
    "owner": "admin",
    "retries": 1,
    "retries_delay": timedelta(minutes=1)
}

dag = DAG("airflow", default_args=default_args, schedule_interval="@once")

def data_clean():
    df = pd.read_csv("/Users/mekhadiuk/df/tiktok_google_play_reviews.csv")
    df.dropna(axis=0, how="any", inplace=True)
    df.fillna("-", inplace=True)
    df["at"] = pd.to_datetime(df["at"])
    df = df.sort_values(by="at")
    emoji = re.compile(
        "["
        "\U0001F1E0-\U0001F1FF"
        "\U0001F300-\U0001F5FF"
        "\U0001F600-\U0001F64F"
        "\U0001F680-\U0001F6FF"
        "\U0001F700-\U0001F77F"
        "\U0001F780-\U0001F7FF"
        "\U0001F800-\U0001F8FF"
        "\U0001F900-\U0001F9FF"
        "\U0001FA00-\U0001FA6F"
        "\U0001FA70-\U0001FAFF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]"
    )

    df["content"].replace(emoji, "", inplace=True)
    df.to_csv("ready.csv")

def upload_data():
    df = pd.read_csv("ready.csv")
    df.to_csv("ready.csv")

first_task = PythonOperator(task_id="load_variables", python_callable=data_clean, dag=dag)
second_task = PythonOperator(task_id="unload", python_callable=upload_data, dag=dag)

first_task >> second_task
