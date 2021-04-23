import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == '__main__':
    sc = SparkContext(appName = 'kafkaspark')
    ssc = StreamingContext(10)
    message = KafkaUtils.createDirectStream(ssc, topics = ['testPK'], kafkaParams = {'metadata.broker.list':
                                                                                     'localhost:9092'})
    words = message.map(lambda x: x[1]).flatMap(lambda x: x.split(" "))
    wordCount = word.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)
    wordCount.pprint()

    ssc.start()
    ssc.awaitTermination()
