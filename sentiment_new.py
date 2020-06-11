import pandas as pd
import requests
import datetime as dt
import psycopg2
# import os.path
from sqlalchemy import create_engine
import config

stocks_df = pd.read_csv('sentiment_stocks.csv')
stocks = stocks_df['Stock'].tolist()

def ticker_sentiment(tickers):
    sentiment_list = []
    time = dt.datetime.today().strftime("%m/%d/%Y %H:%M")
    time = str(time)
    print('Starting Scrape: ', time)
    for ticker in tickers:
        r = requests.get(f'https://stocktwits.com/symbol/{ticker}')
        text = r.text
        sentiment_word = text.find('sentimentChange')
        sentiment = text[sentiment_word + 17:sentiment_word + 23]
        comma = sentiment.find(',')

        if comma != -1:
            sentiment = sentiment[:comma]
            sentiment_list.append([ticker, time, float(sentiment)])
        else:
            sentiment_list.append([ticker, time, 0.0])
    return sentiment_list

def sentiment(stocks):
    stock_sentiments = ticker_sentiment(stocks)
    df_sentiment = pd.DataFrame(stock_sentiments, columns=['ticker', 'date', 'sentiment'])
    time = dt.datetime.today().strftime("%m/%d/%Y %H:%M")
    time = str(time)

    df_sentiment['ticker_id'] = df_sentiment['ticker'].apply(ticker_id_match)

    df_sentiment['created_at'] = time

    df_sentiment.set_index('ticker', inplace=True)

    sql_commit(df_sentiment)

    return df_sentiment


vhinny_ids = pd.read_csv('Tickers_ID.csv')


ticker_id_dict = dict(zip(vhinny_ids.ticker, vhinny_ids.ticker_id))


def ticker_id_match(stock):
    if stock in ticker_id_dict.keys():
        return ticker_id_dict[stock]
    else:
        return None


def sql_commit(df):
    engine = create_engine(f"postgresql://{config.user}:{config.passwd}@{config.host}/{config.db_name}")

    df.reset_index(inplace=True)

    df.to_sql('sentiment', con=engine, if_exists='append', index=False)


print('Starting Script')
sentiment_df = sentiment(stocks)
time = dt.datetime.today().strftime("%m/%d/%Y %H:%M")
time = str(time)
print('Scrape Complete: ', time)







