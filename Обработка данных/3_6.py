from datetime import timedelta, datetime
import pendulum
import random

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator


def hello():
    print('airflow')


def my_print():
    return random.randint(1, 10)


def to_file():
    for _ in range(2):
        n_1 = random.randint(1, 10)
        n_2 = random.randint(1, 10)
        with open('temp.txt', 'a', encoding='utf-8') as file:
            file.write(f'{n_1} {n_2}\n')


def sum_file():
    my_sum_1 = 0
    my_sum_2 = 0
    with open('/root/temp.txt', 'r', encoding='utf-8') as file:
        for now in file:
            now = now.split()
            my_sum_1 += int(now[0])
            my_sum_2 += int(now[1])
    answer = my_sum_1 - my_sum_2
    with open('/root/temp.txt', 'a', encoding='utf-8') as file:
        file.write(str(answer))


default_args = {
    'owner': 'Alexander Brezhnev',
    'email': 'brezhnev.aleksandr@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='3_6',
    schedule_interval='1-5/1 * * * *',
    start_date=pendulum.datetime(2022, 12, 6, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


bash_task = BashOperator(
    task_id='hello', 
    bash_command='echo hello',
    dag=dag
    )
python_task = PythonOperator(
    task_id='world',
    python_callable=hello,
    dag=dag
    )
python_task_2 = PythonOperator(
    task_id='random_number',
    python_callable=my_print,
    dag=dag
    )
python_task_3 = PythonOperator(
    task_id='random_number_to_file',
    python_callable=to_file,
    dag=dag
    )
python_task_4 = PythonOperator(
    task_id='sum_number',
    python_callable=sum_file,
    dag=dag
    )

[bash_task, python_task] >> python_task_2 >> python_task_3 >> python_task_4
