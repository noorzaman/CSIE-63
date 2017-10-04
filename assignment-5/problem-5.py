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

bible_lines = sc.textFile("file:////home/cloudera/Desktop/all-bible")\
              .flatMap(lambda l: l.split())\
	      .map(lambda x: re.sub("[^a-zA-Z]+", "", x.lower().encode("utf-8", "ignore"))) \
	      .filter(lambda x: x != "")

print(bible_lines.take(10))

bible_words = bible_lines.map(lambda p: Row( bible_word=str(p) ) )

print(bible_words.take(10))

bible_df = sqlContext.createDataFrame(bible_words)
print(bible_df.show(10))
bible_df.registerTempTable("KINGJAMES")

bible = sqlContext.sql("""
			SELECT 
			bible_word,
			COUNT(*) freq
			FROM KINGJAMES
			WHERE lower(bible_word) like 'w%'
			AND length(bible_word) > 4 
			GROUP BY bible_word
			HAVING COUNT(*) > 250
			""")

bible = bible.orderBy(bible['freq'].desc())
print(bible.show())

