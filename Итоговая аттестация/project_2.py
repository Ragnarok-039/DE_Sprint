import requests
import pandas as pd
import os
import time
import psycopg2

conn = psycopg2.connect(dbname='brezhnev_alexander_db', user='brezhnev_alexander',
                        password='7Kh9792a', host='localhost')
cursor = conn.cursor()

with open('api_key.txt') as file:
    key = file.readline().strip()
api_key = key
path = r'/root/data-analysis/airflow/dags/jupyter-brezhnev_alexander-9aff9/files/raw_data/'

from_cur = ['USD', 'EUR', 'JPY', 'BTC']
to_cur = 'RUB'

for now in from_cur:
    url = f'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={now}' \
          f'&to_currency={to_cur}&apikey={api_key}'
    r = requests.get(url)
    data = r.json()

    print(data['Realtime Currency Exchange Rate'])
    print('-' * 50)
#     time.sleep(20)

#     df = pd.read_json(url)
#     df.to_csv(f'{path}all_{now}.csv', index=True, encoding='utf-8')
