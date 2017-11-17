# import pandas as pd
# import numpy as np
import matplotlib.pyplot as plt
from mysql.connector import connection
from random import randint
from os import getenv

user = 'moustafa'; password = ''
db = 'stockmarket'
aws_host = ''

config  = {'user': user,
           'password': getenv('MOUSTAFA_PWD') or password,
           'host': getenv('MYSQL_AWS') or aws_host,
           'db': db ,
           'raise_on_warnings': True,
           }

quote = {
    'ask': 'ASK_PRICE',
    'bid': 'BID_PRICE'
    }

tables = {
    'yes':'STOCK_QUOTE_FEED',
    'no':'STOCK_QUOTE'
    }

conn = connection.MySQLConnection(**config)
cursor = conn.cursor()

instr = 3; count = 1500

def formatQuery(qtype, adj):
    return (
        'SELECT {type} FROM {table} WHERE {type} > 0 AND INSTRUMENT_ID = {instr} '.format(
            type=quote[qtype], instr=instr, table=tables[adj]) +
            'ORDER BY QUOTE_TIME, QUOTE_SEQ_NBR {count}'.format(
                count= '' if count==0 else 'LIMIT '+str(count))
            )

def plotData(dataDict):
    for frame in dataDict.keys():
        arr = [col for col in [row for row in dataDict[frame]] ]
        plt.plot(arr, label= frame)

data = {}

cursor.execute(formatQuery('ask', 'yes'))
data['ask_adj'] = cursor.fetchall()

cursor.execute(formatQuery('bid', 'yes'))
data['bid_adj'] = cursor.fetchall()

cursor.execute(formatQuery('ask', 'no'))
data['ask_org'] = cursor.fetchall()

cursor.execute(formatQuery('bid', 'no'))
data['bid_org'] = cursor.fetchall()

cursor.close(); conn.close()

plotData(data); plt.title('INSTR: {}'.format(instr))
plt.xlabel('SEQ_NBR'); plt.ylabel('PRICE')
plt.legend(); plt.show()
