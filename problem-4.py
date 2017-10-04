from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
import re
from operator import add

conf = (
            SparkConf()
            .setAppName("assignment-4")
            .set("spark.executor.instances", 1)
            .set("spark.executor.cores", 1)
            .set("spark.shuffle.compress", "true")
            .set("spark.io.compression.codec", "snappy")
            .set("spark.executor.memory", "4g")
)

sc = SparkContext().getOrCreate(conf = conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)
# spark = SparkSession.builder.appName("spark play").getOrCreate()

ul = sc.textFile("file:////home/cloudera/Desktop/all-bible")
counts = ul.flatMap(lambda x:x.split(" ")).map(lambda x:
(x,1)).reduceByKey(add)

print(counts.take(5))

exchanged = counts.map(lambda x: (x[1],x[0]))
print(exchanged.take(5))
sorted = exchanged.sortBy(lambda x: x[0], ascending=False)
print(sorted.take(20))

shake_ul = sc.textFile("file:////home/cloudera/Desktop/all-shakespeare")
shake_counts = shake_ul.flatMap(lambda x:x.split(" ")).map(lambda x: (x,1)).reduceByKey(add)

print(shake_counts.take(5))

shake_exchanged = shake_counts.map(lambda x: (x[1],x[0]))
print(shake_exchanged.take(5))
shake_sorted = shake_exchanged.sortBy(lambda x: x[0], ascending=False)
print(shake_sorted.take(20))
