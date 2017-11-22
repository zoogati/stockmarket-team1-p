# import pandas as pd
# import numpy as np
import matplotlib.pyplot as plt
#from mpldatacursor import datacursor
from mysql.connector import connection
from random import randint
from os import getenv
from sys import argv

user = 'moustafa'; password = ''
db = 'backup'
aws_host = ''

config  = {'user': user,
           'password': getenv('MOUSTAFA_PWD') or password,
           'host': '127.0.0.1',#getenv('MYSQL_AWS') or aws_host,
           'db': db ,
           'raise_on_warnings': True,
           }

quote = {
    'ask': 'ASK_PRICE',
    'bid': 'BID_PRICE'
    }

tables = {
    'yes':'STOCK_QUOTE_FEED_BK',
    'no':'STOCK_QUOTE_FEED'
    }

conn = connection.MySQLConnection(**config)
cursor = conn.cursor()

try: instr = int(argv[1]);
except: instr= randint(0,29)
try: count = int(argv[2])
except: count= 0

def formatQuery(qtype, adj):
    return (
        'SELECT QUOTE_SEQ_NBR, {type} FROM {table} WHERE {type} > 0 AND INSTRUMENT_ID = {instr} '.format(
            type=quote[qtype], instr=instr, table=tables[adj]) +
            'ORDER BY QUOTE_TIME, QUOTE_SEQ_NBR {count}'.format(
                count= '' if count==0 else 'LIMIT '+str(count))
            )

def plotData(dataDict):
    for frame in dataDict.keys():
        arr = [(num,price) for (num,price) in [row for row in dataDict[frame]] ]
        plt.scatter([el[0] for el in arr], [el[1] for el in arr], label= frame)

data = dict()



cursor.execute(formatQuery('ask', 'no'))
data['ask_HFT'] = cursor.fetchall()

cursor.execute(formatQuery('bid', 'no'))
data['bid_HFT'] = cursor.fetchall()

cursor.execute(formatQuery('ask', 'yes'))
data['ask_before'] = cursor.fetchall()

cursor.execute(formatQuery('bid', 'yes'))
data['bid_before'] = cursor.fetchall()

cursor.close(); conn.close()

plotData(data); plt.title('INSTR: {} -- HFT '.format(instr))
plt.xlabel('SEQ_NBR'); plt.ylabel('PRICE')
plt.grid(color='b', linestyle='--', linewidth=0.2)
plt.legend(); #datacursor()
plt.show()
