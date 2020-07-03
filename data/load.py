import sys
sys.path.insert(0, '..')

from alphaframe.utils.decorators import useray
from os import path
import datetime as dt
import pandas as pd
from csv import reader
import ray




# Load a ticker list from file
def load_ticker_list(path):
    tickers = []
    with open(path, 'r') as csv_file:
        csv_reader = reader(csv_file)
        for row in csv_reader:
            tickers.append(row[0])
    return tickers


# Merge a Dataframe to a ticker data file, remove any duplicate and create the file if necessary
def merge_data_to_file(ticker_path, data):
    if(path.exists(ticker_path)):
        df = pd.read_csv(ticker_path, index_col=0)
        merge = pd.concat([df, data])
        merge.reset_index(inplace=True)
        merge = merge.loc[~merge.duplicated(subset=['date','minute'],keep='first')]
        merge.set_index(['date'], inplace=True)
        merge.to_csv(ticker_path, index=True)
    else:
        data.to_csv(ticker_path, index=True)


# Concatenate two dataframes and sort indexes
@ray.remote
def concat_and_sort(df1,df2):
    return pd.concat([df1, df2]).sort_index()


# Concatenate two dataframes
@ray.remote
def concat(df1,df2):
    return pd.concat([df1, df2],axis=1)


# Load specific ticker data
def load_ticker(data_path, ticker, start_date, end_date, fields='all'):
    df = pd.DataFrame()
    if (fields == 'all'):
        df = pd.read_csv(data_path+ticker+'.csv', parse_dates=[['date','minute']])
    else:
        fields.append('date')
        fields.append('minute')
        df = pd.read_csv(data_path+ticker+'.csv', parse_dates=[['date','minute']],usecols=fields)
    df.fillna(method='ffill', inplace=True)
    df.dropna(inplace=True)
    df.rename(columns={'date_minute':'date'},inplace=True)
    df['asset'] = ticker
    df.set_index(['date', 'asset'],inplace=True)
    df = df.loc[start_date:end_date]
    if df.size == 0:
        print('Dropped '+str(ticker)+' : no data during the asked period')
        return df
    elif df.index[0][0] != start_date:
        print('Dropped '+str(ticker)+' : start is '+df.index[0][0].strftime('%Y-%m-%d %H:%M:%S') + ', missing ' + str((df.index[0][0] - start_date).days)+' days of data.')
        return pd.DataFrame()
    elif  df.index[-1][0] != end_date:
        print('Dropped '+str(ticker)+' : end is '+df.index[-1][0].strftime('%Y-%m-%d %H:%M:%S') + ', missing ' + str((end_date - df.index[-1][0]).days)+' days of data.')
        return pd.DataFrame()
    return df

@useray
def load_tickers(data_path, tickers, start_date, end_date, fields='all'):
    load_ticker_r = ray.remote(load_ticker)

    print("Loading data...")
    frames = []
    for ticker in tickers:
        frames.append(load_ticker_r.remote(data_path,ticker,start_date,end_date,fields))
        if len(frames) > 3:
            ray.wait(frames)

    cnt = 0
    while len(frames) > 1:
        frames = frames[2:] + [concat_and_sort.remote(frames[0],frames[1])]
        if cnt > 3:
            ray.wait(frames)
        else:
            cnt += 1

    res = ray.get(frames[0])
    return res


# Get pricing data for a single ticker
def get_pricing(data_path, ticker, start_date, end_date, field='open'):
    fields=[field, 'date', 'minute']
    df = pd.read_csv(data_path+ticker+'.csv', parse_dates=[['date','minute']],usecols=fields)
    df.fillna(method='ffill', inplace=True)
    df.dropna(inplace=True)
    df.rename(columns={field:ticker, 'date_minute':'Date'}, inplace=True)
    df.set_index(['Date'], inplace=True)
    df = df.loc[start_date:end_date]
    if df.size == 0:
        print('Dropped '+str(ticker)+' : no data during the asked period')
        return df
    elif df.index[0] != start_date:
        print('Dropped '+str(ticker)+' : start is '+df.index[0].strftime('%Y-%m-%d %H:%M:%S') + ', missing ' + str((df.index[0] - start_date).days)+' days of data.')
        return pd.DataFrame()
    elif  df.index[-1] != end_date:
        print('Dropped '+str(ticker)+' : end is '+df.index[-1].strftime('%Y-%m-%d %H:%M:%S') + ', missing ' + str((end_date - df.index[-1]).days)+' days of data.')
        return pd.DataFrame()
    return df


# Get princing data ons et of tickers
@useray
def get_pricings(data_path, tickers, start_date, end_date, field = 'open'):
    get_pricing_r = ray.remote(get_pricing)

    print("Loading prices...")
    frames = []
    for ticker in tickers:
        frames.append(get_pricing_r.remote(data_path,ticker,start_date,end_date, field))
        if len(frames) > 3:
            ray.wait(frames)
    cnt=0
    while len(frames) > 1:
        frames = frames[2:] + [concat.remote(frames[0],frames[1])]
        if cnt > 3:
            ray.wait(frames)
        else:
            cnt += 1

    res = ray.get(frames[0])
    return res
