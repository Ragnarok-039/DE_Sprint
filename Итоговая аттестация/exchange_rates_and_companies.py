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


# Функция для формирования сырого слоя данных курса валют.
# Запрос к API, далее записывается в json файл.
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


# Функция для формирования промежуточного слоя данных в заранее созданной таблице postgres.
# Происходит считывание файла json, выборка необходимых данных и запись в БД.
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


# Функция для формирования сырого слоя данных акций компаний.
# Запрос к API, далее записывается в json файл.
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


# Функция для формирования промежуточного слоя данных в заранее созданной таблице postgres.
# Происходит считывание файла json, выборка необходимых данных и запись в БД.
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


# Функция для формирования витрины данных из промежуточного слоя данных на основе таблицы акций компаний.
# Запрос сделан с помощью SQL.
# В соответствии с заданием №2 сформирован скрипт для заполнения таблицы витрин.
# Данный блок сделан через python оператор, хотя airflow позволяет запускать SQL операторы.
# Но в тестовой версии airflow, к которой нам предоставили доступ, нет доступа к панели администратора.
# Именно в панели администратора настраивается коннектор для БД.
# Также SQL запрос можно сделать отдельным .sql файлом и читать его оттуда.
# Но, при попытке открытия .sql файла, код на тестовом сервере airflow падал с ошибкой.
# Судя по логам, ошибка связана с доступами, значит, необходимо правильно настроить все доступы.
def company_rates_showcase():
    conn = psycopg2.connect(database='brezhnev_alexander_db', user='brezhnev_alexander',
                            password='7Kh9792a', host='146.120.224.166', port='9452')
    cursor = conn.cursor()

    # SQL запрос сформирован с помощью табличных выражений.
    # open_table - курсы валют на момент открытия торгов для данных суток.
    # close_table - курсы валют на момент закрытия торгов для данных суток.
    # total_volume - суммарный объем торгов за последние сутки.
    # max_volume - минимальный временной интервал, на котором был зафиксирован самый крупный объем торгов для данных суток.
    # max_rate - минимальный временной интервал, на котором был зафиксирован максимальный курс для данных суток.
    # min_rate - минимальный временной интервал, на котором был зафиксирован минимальный курс торгов для данных суток.
    # Суррогатный ключ категории получен с помощью хеш-функции путем выборки названия компании и временного интервала.
    cursor.execute("""insert into company_rates_showcase select * from (
                        with open_table as (select * from 
                        (select *, 
                        row_number() over(partition by symbol, date(time_series_30min) order by time_series_30min) as num
                        from company_rates) as temp 
                        where temp.num = 1),

                        close_table as (select * from 
                        (select *, 
                        row_number() over(partition by symbol, date(time_series_30min) order by time_series_30min desc) as num
                        from company_rates) as temp 
                        where temp.num = 1),

                        total_volume as (select symbol, date(time_series_30min) as date, sum(volume) as total_volume
                        from company_rates 
                        group by date(time_series_30min), symbol),

                        max_volume as (select * from 
                        (select *, 
                        row_number() over(partition by symbol, date(time_series_30min) order by volume desc) as num
                        from company_rates) as temp 
                        where temp.num = 1),

                        max_rate as (select * from 
                        (select *, 
                        row_number() over(partition by symbol, date(time_series_30min) order by high desc) as num
                        from company_rates) as temp 
                        where temp.num = 1), 

                        min_rate as (select * from 
                        (select *, 
                        row_number() over(partition by symbol, date(time_series_30min) order by low) as num
                        from company_rates) as temp 
                        where temp.num = 1)

                        select md5(concat(ot.symbol, date(ot.time_series_30min))) as key, 
                        ot.symbol,
                        date(ot.time_series_30min) as date, 
                        tv.total_volume, 
                        ot.open, 
                        ct.close, 
                        ot.open - ct.close as difference, 
                        round((ct.close * 100) / ot.open, 2) as difference_percent, 
                        mv.time_series_30min as largest_trading_volume, 
                        mr.time_series_30min as max_rate, 
                        min_r.time_series_30min as min_rate 
                        from open_table as ot 
                        left join close_table as ct 
                        on ot.symbol = ct.symbol and date(ot.time_series_30min) = date(ct.time_series_30min)
                        left join total_volume as tv
                        on ot.symbol = tv.symbol and date(ot.time_series_30min) = tv.date
                        left join max_volume as mv
                        on ot.symbol = mv.symbol and date(ot.time_series_30min) = date(mv.time_series_30min)
                        left join max_rate as mr
                        on ot.symbol = mr.symbol and date(ot.time_series_30min) = date(mr.time_series_30min)
                        left join min_rate as min_r
                        on ot.symbol = min_r.symbol and date(ot.time_series_30min) = date(min_r.time_series_30min) 
                        where date(ot.time_series_30min) = date(now()) - interval '1 day') as total;""")
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
# Наименование json файла для сырого слоя данных.
file_name = f'{year}_{month}_{day}.json'

# Список валют для конвертации.
from_cur = ['USD', 'EUR', 'JPY', 'BTC', 'SHIB']
# Все конвертируется в рубли.
to_cur = 'RUB'
# Список компаний для получения котировок акций.
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
company_rates_showcase = PythonOperator(
    task_id='company_rates_showcase',
    python_callable=company_rates_showcase,
    dag=dag
)

# Задание очередности выполнения задач.
exchange_rate_to_json >> file_exchange_rate_to_postgres
company_rates_to_json_file >> file_company_rates_to_postgres >> company_rates_showcase
