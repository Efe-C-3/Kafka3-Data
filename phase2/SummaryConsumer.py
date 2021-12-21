import statistics
from kafka import KafkaConsumer, TopicPartition
from json import loads
from statistics import mean


class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        self.ledger = {}
        self.custBalances = {}
        self.deposits = []
        self.withdraws = []


    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            if message['type'] == 'dep':
                self.deposits.append(message['amt'])
                if len(self.deposits) == 0:
                    print('No deposit transaction')
                if len(self.deposits) >= 1:
                    average_deposit = statistics.mean(self.deposits)
                    print('Average deposit is {}'.format(average_deposit))
                if len(self.deposits) >= 2:
                    stdev_deposit = statistics.stdev(self.deposits)
                    print('Standard deviation of deposit is {}'.format(stdev_deposit))
            else:
                self.withdraws.append(message['amt'])
                if len(self.withdraws) == 0:
                    print('No withdraw transaction')
                if len(self.withdraws) >= 1:
                    average_withdraw = mean(self.withdraws)
                    print('Average withdraw is {}'.format(average_withdraw))
                if len(self.withdraws) >= 2:
                    stdev_withdraw = statistics.stdev(self.withdraws)
                    print('Standard deviation of withdraw is {}'.format(stdev_withdraw))


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()