import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mysql.connector import connection
from random import randint

config  = {'user': 'username',
           'password':'pswd',
           'host':'--@aws',
           'db':'stockmarket',
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
        rows = dataDict[frame]
        arr = []
        for row in rows:
            for col in row:
                arr.append(col)
        plt.plot(arr, label= frame)

data = {}

query = formatQuery('ask', 'yes'); cursor.execute(query)
data['ask_adj'] = cursor.fetchall()

query = formatQuery('bid', 'yes'); cursor.execute(query)
data['bid_adj'] = cursor.fetchall()

query = formatQuery('ask', 'no'); cursor.execute(query)
data['ask_org'] = cursor.fetchall()

query = formatQuery('bid', 'no'); cursor.execute(query)
data['bid_org'] = cursor.fetchall()

cursor.close()
conn.close()

plotData(data)
plt.title('INSTR: {}'.format(instr))
plt.xlabel('SEQ_NBR')
plt.ylabel('PRICE')
plt.legend()
plt.show()
