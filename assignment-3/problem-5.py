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

word_file = sc.textFile("file:////home/stirling/assignment-3/ulysses10.txt")
word_count = word_file.flatMap(lambda x: "".join(x).encode("utf-8", "ignore").strip().split()).map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b).values().sum()
print("Total Word Count: {0}".format(str(word_count)))
