# Данный код я использовал в тестовых целях.
# Решил не удалять, так как тут видны первоначальные рассуждения.
# Проба получения данных по api, записи в json, записи в БД.

import requests
import pandas as pd
import os
import time
import psycopg2
import datetime
import dateutil.relativedelta
import json
import os

with open('api_key.txt') as file:
    key = file.readline().strip()
api_key = key
path = r'/root/data-analysis/airflow/dags/jupyter-brezhnev_alexander-9aff9/files/raw_data/'

today = datetime.date.today()
previous_date = today - dateutil.relativedelta.relativedelta(months=1)
year = previous_date.year
month = previous_date.month
day = previous_date.day
file_name = f'{year}_{month}_{day}.json'

from_cur = ['USD', 'EUR', 'JPY', 'BTC', 'SHIB']
to_cur = 'RUB'

with open(f'{path}currency_exchange_rate_{file_name}', 'w') as outfile:
    for now in from_cur:
        url = f'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={now}' \
        f'&to_currency={to_cur}&apikey={api_key}'
        r = requests.get(url)
        data = r.json()
        json_string = json.dumps(data)

        outfile.write(f'{json_string}\n')


print('Open file' * 10)


# with open(f'{path}currency_exchange_rate_{file_name}') as json_file:
#     for line in json_file:
#         temp = json.loads(line)
#         temp = temp['Realtime Currency Exchange Rate']
#         data_list = []
#         for i in temp:
#             data_list.append(temp[i])
# #             print(i, temp[i])
#         print(data_list)

# conn = psycopg2.connect(database='brezhnev_alexander_db', user='brezhnev_alexander',
#                         password='7Kh9792a', host='localhost')
# cursor = conn.cursor()

# cursor.execute(
#   """INSERT INTO currency_exchange_rate
#   (from_currency_code,
#   from_currency_name,
#   to_currency_code,
#   to_currency_name,
#   exchange_rate,
#   last_refreshed,
#   time_zone,
#   bid_price,
#   ask_price)
#   VALUES ('1', '1', '1', '1', '1', '2022-12-29 11:22:38', '1', '1', '1')"""
# )
# conn.commit()
# print("Records inserted successfully")
# conn.close()


with open(f'{path}currency_exchange_rate_{file_name}') as json_file:
    conn = psycopg2.connect(database='brezhnev_alexander_db', user='brezhnev_alexander',
                        password='7Kh9792a', host='localhost')
    cursor = conn.cursor()

    for line in json_file:
        temp = json.loads(line)
        temp = temp['Realtime Currency Exchange Rate']
        data_list = []
        for i in temp:
            data_list.append(temp[i])
#             print(i, temp[i])
        print(data_list)
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


# cursor.execute("SELECT * from currency_exchange_rate")

# rows = cursor.fetchall()
# print('Hello!')
# print(rows)
# conn.close()

#     time.sleep(20)

#     df = pd.read_json(url)
#     df.to_csv(f'{path}all_{now}.csv', index=True, encoding='utf-8')
