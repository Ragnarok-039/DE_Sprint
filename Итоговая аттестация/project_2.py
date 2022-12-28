import requests
import pandas as pd

with open('api_key.txt') as file:
    key = file.readline().strip()
api_key = key

from_cur = 'USD'
to_cur = 'RUB'

url = f'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE' \
      f'&from_currency={from_cur}' \
      f'&to_currency={to_cur}' \
      f'&apikey={api_key}'
r = requests.get(url)
data = r.json()
df = pd.read_json(url)

# print(*[{key: value} for key, value in data['Realtime Currency Exchange Rate'].items()], sep='\n')
print(df['Realtime Currency Exchange Rate'])
print(df.info())
