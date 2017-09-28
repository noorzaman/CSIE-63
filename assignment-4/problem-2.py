from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import *
import re
import string
from bs4 import BeautifulSoup


conf = (
    SparkConf()
        .setAppName("assignment-4")
        .set("spark.executor.instances", 1)
        .set("spark.executor.cores", 1)
        .set("spark.shuffle.compress", "true")
        .set("spark.io.compression.codec", "snappy")
        .set("spark.executor.memory", "4g")
)

sc = SparkContext().getOrCreate(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName("spark play").getOrCreate()
printable = set(string.printable)


### Clean HTML Markup in Text Bible
# with open("/Users/swaite/Stirling/CSIE-63/assignment-4/data/inputs/bible.txt", "r") as myfile:
#     raw_text = myfile.read()
#     clean_bible_text = BeautifulSoup(raw_text, "lxml").text
#
#     with open("/Users/swaite/Stirling/CSIE-63/assignment-4/data/inputs/clean_bible.txt", "w") as text_file:
#         text_file.write(clean_bible_text)

### Stop Words
stop_words_df = spark.read.text("file:////Users/swaite/Stirling/CSIE-63/assignment-4/data/inputs/stop-words.csv")\
#.rdd.flatMap(lambda x: x).collect()
print(stop_words_df.show(10))

### Bible
bible_df = sc.textFile("file:////Users/swaite/Stirling/CSIE-63/assignment-4/data/inputs/clean_bible.txt")\
              .flatMap(lambda x: x.split())\
              .map(lambda x: re.sub("[^a-zA-Z]+", "", x.lower().encode("utf-8", "ignore")))\
              .filter(lambda x: x != "")\
              .map(lambda x: Row(**{'bible_word': str(x)}))\
              .toDF()

combined_bible_df = bible_df.join(stop_words_df, bible_df["bible_word"] == stop_words_df["value"], "left_outer")
bible_non_stop_words_df = combined_bible_df.filter(combined_bible_df["value"].isNull()).select("bible_word")
bible_counted_df = bible_non_stop_words_df.groupBy('bible_word')\
                                          .count() \
                                          .withColumnRenamed("count", "bible_count")  \
                                          .orderBy(col('bible_count').desc())
# List for us 30 most frequent words in each RDD (text). Print or store the words and the numbers of occurrences.
print(bible_counted_df.show(30))

### Ulysses by James Joyce
ulysses_df = sc.textFile("file:////Users/swaite/Stirling/CSIE-63/assignment-4/data/inputs/4300-2.txt") \
               .flatMap(lambda x: x.split()) \
               .map(lambda x: re.sub("[^a-zA-Z]+", "", x.lower().encode("utf-8", "ignore"))) \
               .filter(lambda x: x != "") \
               .map(lambda x: Row(**{'ulysses_word': str(x)})) \
               .toDF()

combined_ulysses_df = ulysses_df.join(stop_words_df, ulysses_df["ulysses_word"] == stop_words_df["value"], "left_outer")
ulysses_non_stop_words_df = combined_ulysses_df.filter(combined_ulysses_df["value"].isNull()).select("ulysses_word")
ulysses_counted_df = ulysses_non_stop_words_df.groupBy('ulysses_word') \
                                              .count() \
                                              .withColumnRenamed("count", "ulysses_count") \
                                              .orderBy(col('ulysses_count').desc())

# List for us 30 most frequent words in each RDD (text). Print or store the words and the numbers of occurrences.
print(ulysses_counted_df.show(30))

# Create for us the list of 20 most frequently used words common to both texts.
print "Create for us the list of 20 most frequently used words common to both texts."
combined_df = bible_counted_df.join(ulysses_counted_df, bible_counted_df['bible_word'] == ulysses_counted_df["ulysses_word"])

# In your report, print (store) the words, followed by the number of occurrences in Ulysses and then the Bible.
print "In your report, print (store) the words, followed by the number of occurrences in Ulysses and then the Bible."
print(combined_df.show(20))
print(combined_df.count())

# In your report, print (store) the words, followed by the number of occurrences in Ulysses and then the Bible.
print "In your report, print (store) the words, followed by the number of occurrences in Ulysses and then the Bible."
bible_combined_df = combined_df.select(['bible_word', 'bible_count']).orderBy(col('bible_count').desc())
print(bible_combined_df.show(20))
print(bible_combined_df.agg(sum('bible_count').alias('sum_bible_count')).show())

# Order your report in descending order starting by the number of occurrences in Ulysses.
print "Order your report in descending order starting by the number of occurrences in Ulysses."
ulysses_combined_df = combined_df.select(['ulysses_word', 'ulysses_count']).orderBy(col('ulysses_count').desc())
print(ulysses_combined_df.show(20))
print(ulysses_combined_df.agg(sum('ulysses_count').alias('sum_ulysses_count')).show())

# List for us a random samples containing 5% of words in the final RDD.
print "List for us a random samples containing 5% of words in the final RDD."
final_df_sample = bible_combined_df.sample(False, 0.5, 13)
print(final_df_sample.show())