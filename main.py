import time
from pprint import pprint
from random import choice
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}


lista = [100,200,404,401,201]

with DAG(
dag_id='minha-primeira-dag',
default_args=args,
schedule_interval="@hourly",    
start_date=days_ago(2),    
tags=['example'],
) as dag:

    # [START howto_operator_python]
    def print_context(ds, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        r = "Deu Certo AMOre"
        print(r)
        return r

    run_this = PythonOperator(
        task_id='inicio_deb',
        python_callable=print_context,
    )
    # [END howto_operator_python]

    # [START howto_operator_python_kwargs]
    def my_sleeping_function(random_base):
        """This is a function that will run within the DAG execution"""
        time.sleep(10)
        print("FELIZ !!!")


    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    for i in range(5):
        task = PythonOperator(
            task_id='dormindo_no_ponto_por_' + str(i),
            python_callable=my_sleeping_function,
            op_kwargs={'random_base': float(i) / 10},
        )
   
        run_this >> task 

    # [END howto_operator_python_kwargs]

   