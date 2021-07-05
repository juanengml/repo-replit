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
    dag_id='req-test-api-http-status',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:

    # [START howto_operator_python]
    def print_context(ds, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        pprint(ds)
        from requests import get 
        r = get("https://httpstatuses.com/200").status_code

        return "api viva " if r == 200 else "api zuada "

    run_this = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
    )
    # [END howto_operator_python]

    # [START howto_operator_python_kwargs]
    def my_sleeping_function(random_base):
        """This is a function that will run within the DAG execution"""
        time.sleep(random_base)
        from requests import get 
        r = get("https://httpstatuses.com/{}".format(choice(lista))).status_code
        pprint(r) 





    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    for i in range(5):
        task = PythonOperator(
            task_id='sleep_for_' + str(i),
            python_callable=my_sleeping_function,
            op_kwargs={'random_base': float(i) / 10},
        )

        run_this >> task 
    # [END howto_operator_python_kwargs]

   