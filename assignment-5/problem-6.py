from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SQLContext, HiveContext, Row
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
hivecontext = HiveContext(sc)

dfs = hivecontext.sql("""
			SELECT 
		        freq,
			lower(word) word 
			FROM kingjames
			WHERE lower(word) like 'w%'
			AND length(word) > 4
			AND freq > 250
			ORDER BY freq DESC
		      """)
print(dfs.show(20))
print(dfs.agg({"freq": "sum"}).show())
print(dfs.count())
