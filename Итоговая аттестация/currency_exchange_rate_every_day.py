import pendulum
import requests
import pandas as pd
import time
import psycopg2
import datetime
import dateutil.relativedelta
import json
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def test():
    print('Hello!', os.getcwd(), os.path.abspath('currency_exchange_rate_every_day.py'))


def exchange_rate_to_json_file(path, file_name, from_cur, to_cur, api_key):
    with open(f'currency_exchange_rate_{file_name}', 'w') as outfile:
        for now in from_cur:
            url = f'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={now}' \
                  f'&to_currency={to_cur}&apikey={api_key}'
            r = requests.get(url)
            data = r.json()
            json_string = json.dumps(data)

            outfile.write(f'{json_string}\n')


def json_file_to_postgres(path, file_name):
    with open(f'currency_exchange_rate_{file_name}') as json_file:
        conn = psycopg2.connect(database='brezhnev_alexander_db', user='brezhnev_alexander',
                                password='7Kh9792a', host='146.120.224.166', port='6080')
        cursor = conn.cursor()

        for line in json_file:
            temp = json.loads(line)
            temp = temp['Realtime Currency Exchange Rate']
            data_list = []

            for i in temp:
                data_list.append(temp[i])

            cursor.execute(
                """INSERT INTO currency_exchange_rate 
                (from_currency_code, 
                from_currency_name, 
                to_currency_code, 
                to_currency_name, 
                exchange_rate, 
                last_refreshed, 
                time_zone, 
                bid_price, 
                ask_price) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", data_list
            )
            conn.commit()

        conn.close()


default_args = {
    'owner': 'Alexander Brezhnev',
    'email': 'brezhnev.aleksandr@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    dag_id='currency_exchange_rate_every_day',
    schedule_interval='30 8 * * *',
    start_date=pendulum.datetime(2022, 12, 29, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
)

api_key = '6LXZ30QB7S2W6EPY'
path = f'{os.path.abspath("currency_exchange_rate_every_day.py")}/raw_data/'

today = datetime.date.today()
previous_date = today - dateutil.relativedelta.relativedelta(months=1)
year = previous_date.year
month = previous_date.month
day = previous_date.day
file_name = f'{year}_{month}_{day}.json'

from_cur = ['USD', 'EUR', 'JPY', 'BTC', 'SHIB']
to_cur = 'RUB'

exchange_rate_to_json = PythonOperator(
    task_id='exchange_rate_to_json',
    python_callable=exchange_rate_to_json_file,
    op_kwargs={'path': path, 'file_name': file_name, 'from_cur': from_cur, 'to_cur': to_cur, 'api_key': api_key},
    dag=dag
)
json_to_postgres = PythonOperator(
    task_id='json_to_postgres',
    python_callable=json_file_to_postgres,
    op_kwargs={'path': path, 'file_name': file_name},
    dag=dag
)
test = PythonOperator(
    task_id='test',
    python_callable=test,
    dag=dag
)

exchange_rate_to_json >> json_to_postgres
