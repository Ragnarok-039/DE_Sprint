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


def exchange_rate_to_json_file(path, file_name, from_cur, to_cur, api_key):
    with open(f'currency_exchange_rate_{file_name}', 'w') as outfile:
        for now in from_cur:
            url = f'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={now}' \
                  f'&to_currency={to_cur}&apikey={api_key}'
            r = requests.get(url)
            data = r.json()
            json_string = json.dumps(data)

            outfile.write(f'{json_string}\n')

            time.sleep(30)


def file_exchange_rate_to_postgres(path, file_name):
    with open(f'currency_exchange_rate_{file_name}') as json_file:
        conn = psycopg2.connect(database='brezhnev_alexander_db', user='brezhnev_alexander',
                                password='7Kh9792a', host='146.120.224.166', port='9452')
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


def company_rates_to_json_file(path, file_name, company_list, api_key):
    for name in company_list:
        with open(f'company_rates_{name}_{file_name}', 'w') as outfile:
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={name}&interval=30min' \
                  f'&apikey={api_key}'
            r = requests.get(url)
            data = r.json()
            json_string = json.dumps(data)

            outfile.write(f'{json_string}\n')

            time.sleep(30)


def file_company_rates_to_postgres(path, file_name, company_list, api_key):
    for name in company_list:
        with open(f'company_rates_{name}_{file_name}') as json_file:
            conn = psycopg2.connect(database='brezhnev_alexander_db', user='brezhnev_alexander',
                                    password='7Kh9792a', host='146.120.224.166', port='9452')
            cursor = conn.cursor()

            temp = json.loads(json_file.read())

            symbol = temp['Meta Data']['2. Symbol']
            time_series = temp['Time Series (30min)']
            for now in time_series:
                my_list = []
                my_list.extend([symbol, now])
                for k in time_series[now]:
                    my_list.append(time_series[now][k])

                cursor.execute(
                    """INSERT INTO company_rates 
                    (symbol, 
                    time_series_30min, 
                    open, 
                    high, 
                    low, 
                    close, 
                    volume) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)""", my_list
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
    dag_id='exchange_rates_and_companies',
    schedule_interval='30 14 * * *',
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
company_list = ['IBM', 'AAPL', 'AMZN', 'GOOG', 'INTC', 'NFLX']

exchange_rate_to_json = PythonOperator(
    task_id='exchange_rate_to_json',
    python_callable=exchange_rate_to_json_file,
    op_kwargs={'path': path, 'file_name': file_name, 'from_cur': from_cur, 'to_cur': to_cur, 'api_key': api_key},
    dag=dag
)
file_exchange_rate_to_postgres = PythonOperator(
    task_id='file_exchange_rate_to_postgres',
    python_callable=file_exchange_rate_to_postgres,
    op_kwargs={'path': path, 'file_name': file_name},
    dag=dag
)
company_rates_to_json_file = PythonOperator(
    task_id='company_rates_to_json_file',
    python_callable=company_rates_to_json_file,
    op_kwargs={'path': path, 'file_name': file_name, 'company_list': company_list, 'api_key': api_key},
    dag=dag
)
file_company_rates_to_postgres = PythonOperator(
    task_id='file_company_rates_to_postgres',
    python_callable=file_company_rates_to_postgres,
    op_kwargs={'path': path, 'file_name': file_name, 'company_list': company_list, 'api_key': api_key},
    dag=dag
)

exchange_rate_to_json >> file_exchange_rate_to_postgres
company_rates_to_json_file >> file_company_rates_to_postgres
