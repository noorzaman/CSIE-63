from __future__ import print_function
from pyspark import SparkConf
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from itertools import islice

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

        ### Initialize streaming context
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
    lines = kvs.map(lambda x: x[1])
    lines.pprint()
    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #     .map(lambda word: (word, 1)) \
    #     .reduceByKey(lambda a, b: a+b)
    # counts.pprint()

    ssc.start()

ssc.awaitTermination()

# python2.7 kafka-orders-consumer.py localhost:9092 p3-new-topic