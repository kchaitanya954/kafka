from kafka import KafkaConsumer
from collections import Counter
import matplotlib.pyplot as plt
consumer = KafkaConsumer('testPK', bootstrap_servers = ['localhost:9092'],
                         auto_offset_reset = 'latest',
                         enable_auto_commit = True
                         )
l = list()
for message in consumer:
    l.append(int(message.value.decode('utf-8')))
    plt.plot(l)
    plt.show()
