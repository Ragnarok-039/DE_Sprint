import requests
import pandas as pd
import os
import time
import psycopg2
import datetime
import dateutil.relativedelta
import json

api_key = '6LXZ30QB7S2W6EPY'

path = r'/root/data-analysis/airflow/dags/jupyter-brezhnev_alexander-9aff9/files/raw_data/'
company_list = ['IBM', 'AAPL', 'AMZN', 'GOOG', 'INTC', 'NFLX']

today = datetime.date.today()
previous_date = today - dateutil.relativedelta.relativedelta(months=1)
year = previous_date.year
month = previous_date.month
day = previous_date.day
file_name = f'{year}_{month}_{day}.json'

for name in company_list:
    with open(f'{path}company_rates_{name}_{file_name}', 'w') as outfile:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={name}&interval=30min' \
              f'&apikey={api_key}'
        r = requests.get(url)
        data = r.json()
        json_string = json.dumps(data)

        outfile.write(f'{json_string}\n')

        time.sleep(15)

print('-' * 50)

for name in company_list:
    with open(f'{path}company_rates_{name}_{file_name}') as json_file:
        conn = psycopg2.connect(database='brezhnev_alexander_db', user='brezhnev_alexander',
                                password='7Kh9792a', host='localhost')
        cursor = conn.cursor()

        temp = json.loads(json_file.read())

        # print(temp)
        print(type(temp), len(temp))
        print(temp['Meta Data']['2. Symbol'])

        symbol = temp['Meta Data']['2. Symbol']
        time_series = temp['Time Series (30min)']
        for now in time_series:
            my_list = []
            my_list.extend([symbol, now])
            for k in time_series[now]:
                my_list.append(time_series[now][k])
            #         print(k)
            #     print(now, time_series[now])
            print(my_list)

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
