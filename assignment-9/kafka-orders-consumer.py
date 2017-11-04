from __future__ import print_function
from pyspark import SparkConf
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from itertools import islice
from datetime import datetime
from operator import add

# Base your Kafka consumer on provided direct_word_count.py script.
# Let Spark streaming engine count the number of orders different stocks where bought in each batch.
# Display for us a section of results in your solution.
# Describe to us the process of installing and invoking Python packages, if any, you needed for this problem.

def parse_order(line):
    # Need to split line into an array
    l = line.split(",")
    try:
        # Getting some none orders wierdness
        # Need to break out
        if l[6] != u"B" and l[6] != u"S":
            raise Exception("Bad line: ({0})".format(line))
        # return parsed line
        return [{
                     "order_date": datetime.strptime(l[0], "%Y-%m-%d %H:%M:%S"),
                     "order_id": long(l[1]),
                     "client_id": long(l[2]),
                     "stock_symbol": l[3],
                     "amount": int(l[4]),
                     "stock_price": float(l[5]),
                     "order_type": l[6]
               }]
    except Exception as err:
        print("Bad line: ({0})".format(line))
        return []

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    # ### Initialize streaming context
    # Base your Kafka consumer on provided direct_word_count.py script.
    # 1. Let Spark streaming engine count the number of orders different stocks where bought in each batch.
    # 2. Display for us a section of results in your solution.
    # 3. Describe to us the process of installing and invoking Python packages,  if any, you needed for this problem.

    conf = SparkConf() \
        .setMaster("local[2]") \
        .setAppName("KafkaSparkStreaming") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.extraClassPath", "./libs/spark-streaming-kafka-0-8-assembly_2.11-2.2.0.jar") \
        .set("spark.executor.extraClassPath", "./libs/spark-streaming-kafka-0-8-assembly_2.11-2.2.0.jar")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    sqlContext = SQLContext(sc)
    spark = SparkSession.builder.appName("spark play").getOrCreate()
    ssc = StreamingContext(sc, 2)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1]) \
               .flatMap(lambda x: [line for line in x.splitlines()])\
               .flatMap(parse_order)

    # https://stackoverflow.com/questions/35582516/spark-counting-distinct-values-by-key
    # Filtering buys by order batch chunks of 1000
    # Let Spark streaming engine count the number of orders different stocks where bought in each batch.
    count_buys = lines.filter(lambda b: b["order_type"] == u"B")\
                      .map(lambda x: (x["stock_symbol"], 1))\
                      .reduceByKey(add)
    # printing stocks sold per order
    count_buys.pprint()

    ssc.start()

ssc.awaitTermination()

# python2.7 kafka-orders-consumer.py localhost:9092 p3-topic-1