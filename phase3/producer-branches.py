import string
from time import sleep
from json import dumps

import psycopg2
from kafka import KafkaProducer
import time
import random


class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: dumps(m).encode('ascii'))

        self.conn = psycopg2.connect(database='kafka', user='postgres', password='postgres', host='localhost', port='5432')
        self.cursor = self.conn.cursor()
        self.cursor.execute("DROP TABLE IF EXISTS branches")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS branches (branchid TEXT, custid INT, type TEXT, date INT, amt INT)")
        self.conn.commit()
        # self.conn.close()

    def emit(self, cust=55, type="dep"):
        data = {'branchid': self.branchid_gen(),
            'custid' : random.randint(50,56),
            'type': self.depOrWth(),
            'date': int(time.time()),
            'amt': random.randint(10,101)*100,
            }
        return data

    def branchid_gen(self, size=8, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for x in range(size))

    def depOrWth(self):
        return 'dep' if (random.randint(0,2) == 0) else 'wth'

    def generateRandomXactions(self, n=1000):
        for _ in range(n):
            data = self.emit()
            print('sent', data)
            self.producer.send('bank-customer-events', value=data)
            sleep(1)
            to_db = (data['branchid'], data['custid'], data['type'], data['date'], data['amt'])
            self.cursor.execute("INSERT INTO branches (branchid, custid, type, date, amt) VALUES (%s,%s,%s,%s,%s);", to_db)
            self.conn.commit()

if __name__ == "__main__":
    p = Producer()
    p.generateRandomXactions(n=20)
