from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

conf = (
            SparkConf()
            .setAppName("assignment-3")
            .set("spark.executor.instances", 1)
            .set("spark.executor.cores", 1)
            .set("spark.shuffle.compress", "true")
            .set("spark.io.compression.codec", "snappy")
            .set("spark.executor.memory", "4g")
)

sc = SparkContext().getOrCreate(conf = conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName("spark play").getOrCreate()

dset = spark.read.text("file:////home/stirling/assignment-3/ulysses10.txt")
print(dset.count())
wordLines = dset.filter(dset.value.contains('afternoon') | dset.value.contains('night') | dset.value.contains('morning'))
print(wordLines.count())
