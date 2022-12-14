import tweepy as tw
import logging
logging.basicConfig(level=logging.INFO)
from common import config
import pandas as pd
import csv
import datetime
import string
from db import create_connection,close_connection

logger = logging.getLogger(__name__)

"""API Autenticator"""
def __oauth_tw():
    __root = config()['twitter']['keys']
    __ak = 'api_key'
    __as = 'api_secrect'
    __tk = 'token'
    __ts = 'token_secrect'
    __br = 'bearer_token'

    """
    auth = tw.OAuth1UserHandler(
        __root[__ak],
        __root[__as],
        __root[__tk],
        __root[__ts]
    )
    api = tw.API(auth)
    """

    client = tw.Client(bearer_token=__root[__br],
                       consumer_key=__root[__ak],
                       consumer_secret=__root[__as],
                       access_token=__root[__tk],
                       access_token_secret=__root[__ts])

    logger.info('OAuth done...')

    #return api
    return client


"""CSV writer"""
def _save_tweets(filename,tweets):
    logger.info('Writing csv...')
    now = datetime.datetime.now().strftime('%Y_%m_%d')
    csv_headers = ['id',
                   'created_date',
                   'text',
                   'source',
                   'in_reply_to_user_id',
                   'user_id',
                   'retweets',
                   'likes',
                   'language']
    name = '{filename}_{date}.csv'.format(filename=filename,date=now)
    file = open(name,'a')
    csvWriter = csv.writer(file,delimiter=';')
    csvWriter.writerow(csv_headers)

    """API v1 - csv writer with fields from api v1"""
    """
    for tweet in tweets:
        csvWriter.writerow([tweet.id, 
                     tweet.created_at,
                     tweet.text.encode('utf-8'),
                     tweet.source,
                     tweet.in_reply_to_status_id,
                     tweet.user.id,
                     tweet.retweet_count,
                     tweet.lang
                     ])
    """

    """API v2 - csv writer with fields from api v2"""
    for tweet in tweets:
        csvWriter.writerow([tweet.id, 
                     tweet.created_at,
                     tweet.text.encode('utf-8'),
                     tweet.source,
                     tweet.in_reply_to_user_id,
                     tweet.author_id,
                     tweet.public_metrics['retweet_count'],
                     tweet.public_metrics['like_count'],
                     tweet.lang
                     ])
    


"""extract tweets of the especify keyword"""
def _tweet_extractor(keyword):
    logger.info('Extracting tweets ...')
    #api = __oauth_tw()
    client = __oauth_tw()

    """API v1 - query"""
    # search = keyword + " -filter:retweets"

    """API v2 - query"""
    search = keyword + " -is:retweet"

    """API v1 - search"""
    #tweets = api.search_tweets(q=search,
    #                           count=100,
    #                           lang='es')

    """API v2 - search"""
    fields = ['public_metrics'
              ,'id'
              ,'created_at'
              ,'text'
              ,'source'
              ,'in_reply_to_user_id'
              ,'author_id'
              ,'lang'
              ]
    tweets_data = client.search_recent_tweets(query=search,
                               max_results=100,
                               tweet_fields=fields
                               )
    tweets = tweets_data.data
    
    filename = 'tweets_for_{word}'.format(word=keyword)
    
    _save_tweets(filename,tweets)


"""create dataframe from csv"""    
def _csv_to_df(filename):
    logger.info('Reading data {}...'.format(filename))
    return pd.read_csv(filename,sep=';')

"""Clean data in csv"""
def _clean_tweets(df):
    logger.info('cleaning tweets data...')

    stripped_text = (df
                    .apply(lambda row: row['text'], axis=1)
                    .apply(lambda text: _clean_tweet_text(text))
    )
    df['text']=stripped_text
    return df

"""Clean text of a tweet"""
def _clean_tweet_text(text):
    if text[:2] == "b'":
        text = text.replace("b'",'')

    if text[1:] == "'":
        text = text[0:len(text)-1]

    return text

"""Remove duplicate tweets"""
def _remove_duplicate_tweets(df, column):
    logger.info('Removing duplicate entries...')
    df.drop_duplicates(subset=[column], keep='first', inplace=True)
    return df

'''Connection to mysql'''
def _db_connection():
    logger.info('Connecting to database...')
    db = create_connection()
    logger.info('Connection to database stablished...')
    return db


def _insert_df_db(df,db,table):
    logger.info('Inserting rows in database')
    df.to_sql(table,con=db,if_exists='append',index=False)


def _close_connection(db):
    logger.info('Closing database sesion...')
    close_connection(db)
    logger.info('Database sesion closed.')


def main():
        keyword = input('Keyword: ')

        _tweet_extractor(keyword=keyword)
        logger.info("Extraction finished.")

        file_path = input('csv path:')
        tweets_df = _csv_to_df(file_path)

        tweets_df = _clean_tweets(tweets_df)

        tweets_df = _remove_duplicate_tweets(tweets_df,'id')

        db =  _db_connection()
        _insert_df_db(tweets_df,db,'tweets')
        _close_connection(db)


if __name__ == '__main__':
    main()