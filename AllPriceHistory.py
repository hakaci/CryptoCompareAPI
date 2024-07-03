import requests as rq
import pandas as pd
from datetime import datetime as dt
from pathlib import Path
import time as t
from pandas.io.json import json_normalize

url_price = "https://min-api.cryptocompare.com/data/v2/histoday?"

# Historical Prices
api_key = "-"
fsym = ""
tsym = "USD"
e = "CCCAGG"
allData = ""
toTs = int(dt.utcnow().timestamp() - 86400)

path_coinmeta = Path("F:/Bitcoin_blockchain_data/CryptoCompare API/CryptoCompare_coin_meta.csv")
coinmeta = pd.read_csv(path_coinmeta)

coins = {'BTC', 'XRP', 'ETH', 'USDT', 'BCH', 'LTC', 'EOS', 'LINK', 'BSV', 'XMR', 'DASH', 'ETC'}

for coin in coins:
    coinmeta_temp = coinmeta[coinmeta['Symbol'] == coin]
    coinId = int(coinmeta_temp['Id'])
    Params = {
        'fsym': coin,
        'tsym': tsym,
        'api_key': api_key,
        'allData': 'true'
    }
    # Get data
    result = rq.get(url_price, Params)
    t.sleep(0.1)
    result_j = result.json()
    if result_j['Response'] == "Success":
        result_df = json_normalize(result_j['Data']['Data'])
        # Recrate DataFrame
        result_df.drop(columns={'conversionType', 'conversionSymbol'}, inplace=True)
        # create Id, Market
        result_df.insert(loc=0, value=e, column='market')
        result_df.insert(loc=0, value=coinId, column='Id')
        # Append to excel
        result_df.to_csv("C:/Users/hakaci/Desktop/prices_test.csv", index=False, mode='a',
                         header=False)
    else:
        print("Error")
print("finish")
