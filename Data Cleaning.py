import pandas as pd
from pathlib import Path


# RSI function # Değerler tam doğru değil
def _rsi(close, rsi_period):
    gain = close.mask(close < 0, 0)
    loss = close.mask(close > 0, 0)
    avg_gain = gain.ewm(com=rsi_period-1, min_periods=rsi_period, adjust=False).mean()
    avg_loss = loss.ewm(com=rsi_period-1, min_periods=rsi_period, adjust=False).mean()
    rs = abs(avg_gain / avg_loss)
    rs.rename('rsi', inplace=True)
    return 100 - (100 / (1 + rs))


# MACD function
#  min_periods = EMA_fast_n,
def _macd(close, ema_fast_n, ema_slow_n):
    ema_fast = close.ewm(com=ema_fast_n-1, min_periods=ema_fast_n, adjust=False).mean()
    ema_slow = close.ewm(com=ema_slow_n-1, min_periods=ema_slow_n, adjust=False).mean()
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(com=9-1, min_periods=9, adjust=False).mean()
    macd_diff = macd - macd_signal

    new_frame = {'MACD': macd, 'MACD_signal': macd_signal, 'MACD_diff': macd_diff}
    df = pd.DataFrame(new_frame)
    return df


# tail, head
n = 50
rsi_period = 14
EMA_fast_n = 12
EMA_slow_n = 26
# Default market
market = 'CCCAGG'

# Çekilecek coinler: BTC, XRP, ETH, USDT, BCH, LTC,  EOS, LINK, BSV, XLM, XMR, DASH, ETC
coins = {'BTC', 'XRP', 'ETH', 'USDT', 'BCH', 'LTC', 'EOS', 'LINK', 'BSV', 'XMR', 'DASH', 'ETC'}

indicator_rsi = pd.DataFrame()
indicator_macd = pd.DataFrame()

path_to_prices = Path("F:/Bitcoin_blockchain_data/CryptoCompare API/prices_test_new.csv")
path_coinmeta = Path("F:/Bitcoin_blockchain_data/CryptoCompare API/CryptoCompare_coin_meta.csv")
path_rsi = Path("F:/Bitcoin_blockchain_data/CryptoCompare API/indicator_rsi.csv")
path_macd = Path("F:/Bitcoin_blockchain_data/CryptoCompare API/indicator_macd.csv")

prices = pd.read_csv(path_to_prices)
coinmeta = pd.read_csv(path_coinmeta)
prices.set_index('Id', inplace=True)

for coin in coins:
    coinmeta_temp = coinmeta[coinmeta['Symbol'] == coin]
    Id_temp = int(coinmeta_temp['Id'])
    prices_smp = prices.loc[Id_temp]
    prices_smp = prices_smp[['market', 'time']]

    # rsi generate and insert
    change = prices.loc[Id_temp, 'close'].diff()
    # İlk satır boş kalıyor. Grafik şekli aynı ama değerler farklı
    # change = change[1:]
    change.fillna(0, inplace=True)
    rsi = _rsi(change, rsi_period)
    # Join
    indicator_rsi_tmp = pd.concat([prices_smp, rsi], axis=1)

    # MACD generate and insert
    # Generate
    close = prices.loc[Id_temp, 'close']
    macd_df = _macd(close, EMA_fast_n, EMA_slow_n)
    # Join
    indicator_macd_tmp = pd.concat([prices_smp, macd_df], axis=1)
    # Insert
    if not indicator_rsi_tmp.empty:
        indicator_rsi = indicator_rsi.append(indicator_rsi_tmp)
    if not indicator_macd_tmp.empty:
        indicator_macd = indicator_macd.append(indicator_macd_tmp)

# Exporting to csv
indicator_rsi.to_csv(path_rsi)
indicator_macd.to_csv(path_macd)
