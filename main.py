import requests as rq
import pandas as pd
from datetime import datetime as dt
from pathlib import Path
import time as t
from pandas.io.json import json_normalize

# Statics
api_key = "-"
fsym = ""
tsym = "USD"
e = "CCCAGG"
allData = ""
toTs = ""

url_coinmeta = "https://min-api.cryptocompare.com/data/all/coinlist"
url_market = "https://min-api.cryptocompare.com/data/v3/all/exchanges"
url_price = "https://min-api.cryptocompare.com/data/v2/histoday?"

path_coinmeta = Path("F:/Bitcoin_blockchain_data/CryptoCompare API/CryptoCompare_coin_meta.csv")
path_market = Path("F:/Bitcoin_blockchain_data/CryptoCompare API/CryptoCompare_market.csv")
path_prices = Path("F:/Bitcoin_blockchain_data/CryptoCompare API/prices_test_new.csv")
# path_test = Path("F:/Bitcoin_blockchain_data/CryptoCompare API/update_tester.csv")
# path_test = Path("C:/Users/hakaci/Desktop/prices_test.csv")

update_row_header = pd.DataFrame()
utc_now = dt.utcnow()
toTs = int(utc_now.timestamp() - 86400)
# Çekilecek coinler: BTC, XRP, ETH, USDT, BCH, LTC,  EOS, LINK, BSV, XLM, XMR, DASH, ETC
coins = {'BTC', 'XRP', 'ETH', 'USDT', 'BCH', 'LTC', 'EOS', 'LINK', 'BSV', 'XMR', 'DASH', 'ETC'}

coinmeta = rq.get(url_coinmeta)
t.sleep(0.1)
market = rq.get(url_market)
t.sleep(0.1)

# Format Data
coinmeta_j = coinmeta.json()
coinmeta_df = pd.DataFrame(coinmeta_j['Data'])
coinmeta_df = coinmeta_df.T

market_j = market.json()
market_df = pd.DataFrame(market_j['Data'])
market_df = market_df.T
market_df.reset_index(level=0, inplace=True)
market_df.rename(columns={"index": "market"}, inplace=True)

# Fill the csv
coinmeta_df.to_csv(path_coinmeta, index=False)
market_df.to_csv(path_market, index=False, columns=['market', 'isActive', 'isTopTier'])

# top100 = df[df['SortOrder'] <= 100]
# df_market = pd.read_csv(path_market)
# topMarket = df_market[(df_market['isTopTier'] == True) & (df_market['isActive'] == True)]
# # For update add CCGCC
# topMarket = topMarket.append({'market': 'CCCAGG', 'isActive': True, 'isTopTier': True}, ignore_index=True)
prices = pd.read_csv(path_prices, usecols=['Id', 'market', 'time'])
prices = prices[prices['time'] >= int(utc_now.timestamp() - 8640000)]


# Güncellenecek template satırlar
if not prices.empty:
    for coin in coins:
        coinmeta_df_temp = coinmeta_df[coinmeta_df['Symbol'] == coin]
        Id_temp = int(coinmeta_df_temp['Id'])
        Symbol_temp = coin
        dfresult_temp = prices[prices['Id'] == Id_temp]
        dfresult_result_tempdf = dfresult_temp[(dfresult_temp['time'] == dfresult_temp['time'].max())]
        dfresult_result_tempdf.insert(loc=1, value=Symbol_temp, column='Symbol')
        if not dfresult_result_tempdf.empty:
            update_row_header = update_row_header.append(dfresult_result_tempdf)

    # Requests
    for row in update_row_header.itertuples():
        Id = getattr(row, 'Id')
        Symbol = getattr(row, 'Symbol')
        time = getattr(row, 'time')

        update_days_diff = abs(utc_now - dt.fromtimestamp(time))
        update_days_diff_int = int(update_days_diff.days) - 1
        if not update_days_diff_int <= 0:
            params_update = {
                'fsym': Symbol,
                'tsym': tsym,
                'api_key': api_key,
                'limit': update_days_diff_int,
                'toTs': toTs
            }
            update = rq.get(url_price, params_update)
            t.sleep(0.1)
            update_j = update.json()
            if update_j['Response'] == "Success":
                update_df = json_normalize(update_j['Data']['Data'])
                update_df.drop(columns={'conversionType', 'conversionSymbol'}, inplace=True)
                update_df.insert(loc=0, value=e, column='market')
                update_df.insert(loc=0, value=Id, column='Id')
                update_df = update_df.drop([0])
                print(update_df)
                update_df.to_csv(path_prices, index=False, mode='a', header=False)
            else:
                print("Request fail.")
        else:
            print("No need update for coin: " + getattr(row, 'Symbol'))
else:
    print("prices is empty")
print("\nfinish\n")
