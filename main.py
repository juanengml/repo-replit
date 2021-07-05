from datetime import datetime
from pathlib import Path
from datetime import datetime as dt 
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="request_endpoint_event",
    schedule_interval="@daily",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 1, 5),
)

endpoint = "http://ec2-54-91-136-29.compute-1.amazonaws.com:8080/status"

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /tmp/events ; "
        "curl {} > /tmp/evento.json ; cat /tmp/evento.json ; ".format(endpoint)
        
       ),
    dag=dag,
)


def _trata_json(input_path, output_path):
    """Calculates event statistics."""
    import json
     # Opening JSON file
    f = open(input_path)
    data = json.load(f)
    status = data['appState']['yoloStatus']
    f.close()
    import csv
    # open the file in the write mode
    with open(output_path, 'w') as f:
    # create the csv writer
       writer = csv.writer(f)
       print(status, dt.now())
       writer.writerow(status)
   


trata_json = PythonOperator(
    task_id="trata_json",
    python_callable=_trata_json,
    op_kwargs={"input_path": "/tmp/evento.json", "output_path": "/tmp/stats.csv"},
    dag=dag,
)

fetch_events >> trata_json