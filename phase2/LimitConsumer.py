from kafka import KafkaConsumer, TopicPartition
from json import loads


class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        self.ledger = {}
        self.custBalances = {}

    def handleMessages(self):
        for message in self.consumer:
            message = message.value     #  values in the table are taken from the dictionary by using the .values() method
            print('{} received'.format(message))
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
                print(self.custBalances)
            else:
                if self.custBalances[message['custid']] <= -100:
                    print('Not enough balance')
                    print(self.custBalances)
                    continue
                else:
                    self.custBalances[message['custid']] -= message['amt']
                    print(self.custBalances)


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()