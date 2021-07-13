import time
from pprint import pprint
from random import choice
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
import pandas as pd 
from requests import get 
from os import system

#import firebase_admin
#from firebase_admin import credentials
#from firebase_admin import db

args = {
    'owner': 'airflow',
}


lista = [100,200,404,401,201]

with DAG(
dag_id='etl-poc-experimento',
default_args=args,
schedule_interval="@hourly",    
start_date=days_ago(2),    
tags=['api-rest-etl'],
) as dag:

    # [START howto_operator_python]
    def extracao_api():
        try:
          system("pip --version")
          #system("pip install firebase-admin")

        except:
          pass 

        """Print the Airflow context and ds variable from the context."""
        r = "Deu Certo AMOre"
        endpoint = "https://api-gt-services.juanengml.repl.co/api/v1/users/ativos"
        r = get(endpoint).json()
        df = pd.DataFrame(r)
        df.to_csv("/tmp/base.csv",index=False)
        print(df.head())
        return r

    extracao_api_task = PythonOperator(
        task_id='extracao_api_rest',
        python_callable=extracao_api,
    )
    # [END howto_operator_python]

    # [START howto_operator_python_kwargs]
    def transformacao():
        """This is a function that will run within the DAG execution"""
        time.sleep(10)
        print("READ CSV AND TRANSFORM DATA FOR NEW CSV_FINAL")
        base = pd.read_csv("/tmp/base.csv")
        print(base.head()) 
        
        print("gerar base_rede_social.csv")
        base_rede_social = base.query("site != 'www.g1.com.br'").query("site != 'www.g1.globo.com.br'").query("site != 'www.oestadao.com.br'")
        base_rede_social.to_csv("/tmp/base_rede_social.csv",index=False)
        
        print("gerar base_noticias.csv")
        base_noticias = base.query("site != 'www.facebook.com'").query("site != 'www.twitter.com'").query("site != 'www.tiktok.com'")
        base_noticias.to_csv("/tmp/base_noticias.csv",index=False)
        print("FELIZ !!!")

    transformaca_api_split_task = PythonOperator(
            task_id='transformacao_split_data',
            python_callable=transformacao
            
        )
    def load():
        """This is a function that will run within the DAG execution"""
        time.sleep(10)
        print("LOAD DADOS IN DB ")

     
    load_data = PythonOperator(
            task_id='load_data_to_db_firebase',
            python_callable=load
            
        )    
    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    
    extracao_api_task >> transformaca_api_split_task  >> load_data

    # [END howto_operator_python_kwargs]

   