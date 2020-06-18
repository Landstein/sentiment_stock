import requests
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
import os
import config

dir_path = os.path.dirname(os.path.realpath(__file__))
st_tickers = pd.read_csv('2020-06-18.csv')
tickers = st_tickers['AAC'].tolist()

def ticker_followers(tickers):
    follower_list = []
    time = dt.datetime.today().strftime("%m/%d/%Y %H:%M")
    time = str(time)
    print('Starting Scrape: ', time)
    count = 0
    for ticker in tickers:
        count += 1
        r = requests.get(f'https://stocktwits.com/symbol/{ticker}')
        text = r.text
        follower_list.append([ticker, time, extract_followers(text)])
        print(count, ticker)

    return follower_list


def extract_followers(text):
    word = text.find('strong')
    if word != -1:
        followers = text[word+7: word+16]
        left_arrow = followers.find('<')
        watchers = followers[0:left_arrow].replace(',', '')
        return int(watchers)
    else:
        return 0


def watchers(stocks):
    stock_follower = ticker_followers(stocks)
    df_watchers = pd.DataFrame(stock_follower, columns=['ticker', 'date', 'watchers'])
    time = dt.datetime.today().strftime("%m/%d/%Y %H:%M")
    time = str(time)

    df_watchers['ticker_id'] = df_watchers['ticker'].apply(ticker_id_match)

    df_watchers['created_at'] = time

    df_watchers.set_index('ticker', inplace=True)

    sql_commit(df_watchers)

    return df_watchers


vhinny_ids = pd.read_csv(dir_path + '/' + 'Tickers_ID.csv')


ticker_id_dict = dict(zip(vhinny_ids.ticker, vhinny_ids.ticker_id))


def ticker_id_match(stock):
    return ticker_id_dict.get(stock)


def sql_commit(df):
    engine = create_engine(f"postgresql://{config.user}:{config.passwd}@{config.host}/{config.db_name}")

    df.reset_index(inplace=True)

    df.to_sql('watchers', con=engine, if_exists='append', index=False)


print('Starting Script')
watchers_df = watchers(tickers)
time = dt.datetime.today().strftime("%m/%d/%Y %H:%M")
time = str(time)
print('Scrape Complete: ', time)



