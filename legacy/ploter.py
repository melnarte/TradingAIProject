import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib import style
from mpl_finance import candlestick_ohlc


def ohlcv(df,indicators=[],signals=[]):
    style.use('ggplot')
    df_ohlc = df[['open', 'high', 'low', 'close']]
    df_volume = df['volume']
    df_ohlc.reset_index(inplace=True)
    df_ohlc.drop(columns=['ticker'], inplace=True)
    df_ohlc['date_minute'] = df_ohlc['date_minute'].map(mdates.date2num)
    df_volume.reset_index(level=1,drop=True,inplace=True)



    ax1 = plt.subplot2grid((6,1), (0,0), rowspan = 5, colspan=1)
    ax2 = plt.subplot2grid((6,1), (5,0), rowspan=1, colspan=1, sharex=ax1)

    ax1.xaxis_date()
    candlestick_ohlc(ax1, df_ohlc.values, width=0.6/(24*60), colorup='g')

    df_ind = df[indicators]
    df_ind.reset_index(inplace=True)
    df_ind.drop(columns=['ticker'], inplace=True)
    for indicator in indicators:
        ax1.plot(df_ohlc['date_minute'],df_ind[indicator], label=indicator)
    ax2.fill_between(df_volume.index.map(mdates.date2num), df_volume.values, 0)
    plt.show()
