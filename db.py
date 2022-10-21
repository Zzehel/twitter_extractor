from sqlalchemy import create_engine
import pymysql
from common import config
import pandas as pd

_db_settings = config()['database']

_host = _db_settings['host']
_port = _db_settings['port']
_user = _db_settings['user']
_pass = _db_settings['pass']
_db = _db_settings['db']

def create_connection():
    #'dialect+driver://username:password@host:port/database'
    engine = create_engine(url='mysql+pymysql://{user}:{password}@{host}:{port}/{db}'.format(
        user=_user,password=_pass,host=_host,port=_port,db=_db))
    
    dbConnection = engine.connect()

    return dbConnection


def close_connection(db):
    db.close()