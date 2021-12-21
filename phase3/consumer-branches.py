import psycopg2
from kafka import KafkaConsumer, TopicPartition
from json import loads
from sqlalchemy import create_engine
# from mysql.connector import (connection)

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        self.ledger = {}
        self.custBalances = {}
        self.conn = psycopg2.connect(database='kafka', user='postgres', password='postgres', host='localhost', port='5432')
        self.cursor = self.conn.cursor()
        self.cursor.execute("DROP TABLE IF EXISTS transaction")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS transaction (branchid TEXT, custid INT, type TEXT, date INT, amt INT)")
        self.conn.commit()
        # self.conn.close()

    def handleMessages(self):
        for message in self.consumer:
            message = message.value     #  values in the table are taken from the dictionary by using the .values() method
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            for message['custid'] in self.ledger:
                to_db = (message['branchid'], message['custid'], message['type'], message['date'], message['amt'])
                self.cursor.execute("INSERT INTO transaction (branchid, custid, type, date, amt) VALUES (%s,%s,%s,%s,%s);", to_db)
                self.conn.commit()
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)
            # self.conn.close()

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()