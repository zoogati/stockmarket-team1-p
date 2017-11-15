# import sshtunnel
from mysql.connector import connection
import csv, itertools, time, math

config = {
  'user': 'username',
  'password': 'pwd',
  'host': '--db--host@aws',
  'database': 'stockmarket',
  'raise_on_warnings': True,
}


db = connection.MySQLConnection(**config)
cursor = db.cursor()

query = ('INSERT INTO STOCK_QUOTE VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)')
with open ('STOCK_QUOTE.csv', newline='') as csvfile:
    csvfile.readline()
    start = time.time()
    count  = 0
    for quote in csv.reader(csvfile, delimiter=';', quoting=csv.QUOTE_NONNUMERIC):
        cursor.execute(query, tuple(quote))
        count +=1
        if count%1000 == 0 :
            db.commit()
            now = time.time()
            rate = math.ceil(1000/(now-start))
            start = now
            print ('Records processed: {} | Rate: {}/s'.format(count,rate))

db.commit()
print ('Total: {} records'.format(count))
cursor.close()
db.close()
