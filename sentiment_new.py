import pandas as pd
import requests
import datetime as dt
from sqlalchemy import create_engine
import os
import config

dir_path = os.path.dirname(os.path.realpath(__file__))

stocks_df = pd.read_csv(dir_path + '/' + 'sentiment_stocks.csv')
stocks = stocks_df['Stock'].tolist()

def ticker_sentiment(tickers):
    sentiment_list = []
    time = dt.datetime.today().strftime("%m/%d/%Y %H:%M")
    time = str(time)
    print('Starting Scrape: ', time)
    for ticker in tickers:
        r = requests.get(f'https://stocktwits.com/symbol/{ticker}')
        text = r.text
        sentiment_list.append([ticker, time, extract_volume(text), extract_sentiment(text)])
    return sentiment_list


def extract_sentiment(text):
    sentiment_word = text.find('sentimentChange')
    sentiment = text[sentiment_word + 17:sentiment_word + 23]
    sentiment_comma = sentiment.find(',')

    if sentiment_comma != -1:
        sentiment = sentiment[:sentiment_comma]
        return float(sentiment)
    else:
        return 0.0


def extract_volume(text):
    volume_word = text.find("volumeChange")
    volume = text[volume_word + 14:volume_word + 22]
    volume_comma = volume.find(',')

    if volume_comma != -1:
        volume = volume[:volume_comma]
        return float(volume)
    else:
        return 0.0

def sentiment(stocks):
    stock_sentiments = ticker_sentiment(stocks)
    df_sentiment = pd.DataFrame(stock_sentiments, columns=['ticker', 'date', 'msg_volume', 'sentiment'])
    time = dt.datetime.today().strftime("%m/%d/%Y %H:%M")
    time = str(time)

    df_sentiment['ticker_id'] = df_sentiment['ticker'].apply(ticker_id_match)

    df_sentiment['created_at'] = time

    df_sentiment.set_index('ticker', inplace=True)

    sql_commit(df_sentiment)

    return df_sentiment


vhinny_ids = pd.read_csv(dir_path + '/' + 'Tickers_ID.csv')


ticker_id_dict = dict(zip(vhinny_ids.ticker, vhinny_ids.ticker_id))


def ticker_id_match(stock):
    return ticker_id_dict.get(stock)


def sql_commit(df):
    engine = create_engine(f"postgresql://{config.user}:{config.passwd}@{config.host}/{config.db_name}")

    df.reset_index(inplace=True)

    df.to_sql('sentiment', con=engine, if_exists='append', index=False)


print('Starting Script')
sentiment_df = sentiment(stocks)
time = dt.datetime.today().strftime("%m/%d/%Y %H:%M")
time = str(time)
print('Scrape Complete: ', time)








