import pandas as pd
import math
import os.path
import time
# from bitmex import bitmex
from binance.client import Client
from datetime import timedelta, datetime
from dateutil import parser

DATA_PATH = "/Users/taoxing/Desktop/Bahamut/data/CoinData/main"

### API

binance_api_key = 'jGdPOjC7CBYI9CHZRxQ5Txs0ln2yo1nbfEdNutvaS5ZgDNzTi3BqLJGmkTiGiMVQ'    #Enter your own API-key here
binance_api_secret = 'X2OC0RucVGlHlUsW3gpLZ3Lo3TPnsZhofmqCL3VuuUdJMSaImQuHbwqo4Ih3Jq9g' #Enter your own API-secret here

### CONSTANTS
binsizes = {"1m": 1, "5m": 5, "1h": 60, "1d": 1440}
batch_size = 750

### FUNCTIONS
def minutes_of_new_data(symbol, kline_size, data, source):
    # 设置开始时间
    if len(data) > 0:  
        # 若有现有数据，则以现有数据的最后时间为开始时间
        old = parser.parse(data["timestamp"].iloc[-1])
    elif source == "binance": 
        old = datetime.strptime('1 Jan 2017', '%d %b %Y')
    elif source == "bitmex": 
        old = bitmex_client.Trade.Trade_getBucketed(symbol=symbol, binSize=kline_size, count=1, reverse=False).result()[0][0]['timestamp']
    
    # 获取最新的时间
    if source == "binance": 
        new = pd.to_datetime(binance_client.get_klines(symbol=symbol, interval=kline_size)[-1][0], unit='ms')   
    if source == "bitmex": 
        new = bitmex_client.Trade.Trade_getBucketed(symbol=symbol, binSize=kline_size, count=1, reverse=True).result()[0][0]['timestamp']  
    return old, new

def get_price(symbol, kline_size, localdata = False):
    '''下载K线数据'''
    # 定义下载数据的文件名称
    # E:\study\tensortrade-master\downloaded\CoinData
    filename = DATA_PATH+'/binance-%s-%s.csv' % (symbol, kline_size)

    # 是否使用本地数据
    if localdata == True:
        # 判断数据是否存在
        if os.path.isfile(filename): 
            data_df = pd.read_csv(filename)
        else: 
            data_df = pd.DataFrame()
            print ("EORRR!")
            return data_df

    elif localdata == False:
        binance_client = Client(api_key=binance_api_key, api_secret=binance_api_secret)
        # 判断数据是否存在
        if os.path.isfile(filename): 
            data_df = pd.read_csv(filename)
        else: 
            data_df = pd.DataFrame()
    
        oldest_point, newest_point = minutes_of_new_data(symbol, kline_size, data_df, source = "binance")
        delta_min = (newest_point - oldest_point).total_seconds()/60
        available_data = math.ceil(delta_min/binsizes[kline_size])
        
        if oldest_point == datetime.strptime('1 Jan 2017', '%d %b %Y'): 
            print('Downloading all available %s data for %s. Be patient..!' % (kline_size, symbol))
        else: 
            print('Downloading %d minutes of new data available for %s, i.e. %d instances of %s data.' % (delta_min, symbol, available_data, kline_size))
        
        klines = binance_client.get_historical_klines(symbol, kline_size, oldest_point.strftime("%d %b %Y %H:%M:%S"), newest_point.strftime("%d %b %Y %H:%M:%S"))
        data = pd.DataFrame(klines, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ])
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
        # 若数据存在，则补充最新的数据
        if len(data_df) > 0:
            temp_df = pd.DataFrame(data)
            data_df = data_df.append(temp_df)
        else: 
            data_df = data

    data_df.set_index('timestamp', inplace=True)
    data_df.index = pd.to_datetime(data_df.index, format = "%Y-%m-%d %H:%M:%S")

    data_df['date'] = data_df.index.values
    data_df['open'] = data_df['open'].astype('float64')
    data_df['high'] = data_df['high'].astype('float64')
    data_df['low'] = data_df['low'].astype('float64')
    data_df['close'] = data_df['close'].astype('float64')
    data_df['volume'] = data_df['volume'].astype('float64')

    print('All caught up..!')
    return data_df

def get_price_time(symbol, kline_size, start, end):
    '''下载start到end之间的历史数据'''
    import numpy as np
    s_date = np.datetime64(start)
    e_date = np.datetime64(end)

    price = get_price(symbol, kline_size, True)
    df_price = price[(price.index >= s_date) & (price.index <= e_date)]
    return df_price

def get_OHLCV(symbol, kline_size, start, end):
    '''下载start到end之间的K线数据'''
    import numpy as np
    s_date = np.datetime64(start)
    e_date = np.datetime64(end)

    price = get_price(symbol, kline_size, True)
    df_price = price[(price.index >= s_date) & (price.index <= e_date)]
    df_price = df_price[['date','open','high','low','close','volume']]
    df_price.columns = ['Date','Open','High','Low','Close','Volume']
    return df_price

def get_OHLC(symbol, kline_size, start, end):
    '''下载start到end之间的K线数据'''
    import numpy as np
    s_date = np.datetime64(start)
    e_date = np.datetime64(end)

    price = get_price(symbol, kline_size, True)
    df_price = price[(price.index >= s_date) & (price.index <= e_date)]
    df_price = df_price[['date','open','high','low','close']]
    df_price.columns = ['Date','Open','High','Low','Close']
    return df_price

def get_ohlcv(symbol, kline_size, start, end):
    '''下载start到end之间的K线数据'''
    import numpy as np
    s_date = np.datetime64(start)
    e_date = np.datetime64(end)

    price = get_price(symbol, kline_size, True)
    df_price = price[(price.index >= s_date) & (price.index <= e_date)]
    df_price = df_price[['date','open','high','low','close','volume']]
    return df_price