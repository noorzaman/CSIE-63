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

def hasWords(line):
        return "afternoon" in line or "night" in line or "morning" in line

lines = sc.textFile("file:////home/stirling/assignment-3/ulysses10.txt")

wordLines = lines.filter(hasWords)
print("Problem 3 filter w/ function count: {0}".format(str(wordLines.count())))

wordLines = lines.filter(lambda line: "afternoon" in line or "night" in line or "morning" in line)
print("Problem 3 filter w/ non function filter count: {0}".format(str(wordLines.count())))

