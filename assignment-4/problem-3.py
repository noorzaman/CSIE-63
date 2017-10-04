from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
import re


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

#Consider attached files transactions.txt and products.txt.

# Each line in transactions.txt file contains a
#       transaction date,
#       time,
#       customer id,
#       product id,
#       quantity bought and
#       price paid,
#
# delimited with hash (#) sign.

transactions_rdd = sc.textFile("file:////Users/swaite/Stirling/CSIE-63/assignment-4/data/inputs/transactions.txt") \
                     .map(lambda x: x.split("#"))
transactions_rdd = transactions_rdd.map(lambda x:
                                        Row(
                                            transaction_date=str(x[0]),
                                            time=str(x[1]),
                                            customer_id=int(x[2]),
                                            product_id=int(x[3]),
                                            quantity_bought=int(x[4]),
                                            price_paid=float(x[5])
                                        ))
transactions_df = spark.createDataFrame(transactions_rdd)
print(transactions_df.show(10))


# Each line in file products.txt contains:
#       product id,
#       product name,
#       unit price,
#       quantity
# available in the store.
# Bring those data in Spark and organize it as DataFrames with named columns.

products_rdd = sc.textFile("file:////Users/swaite/Stirling/CSIE-63/assignment-4/data/inputs/products.txt")\
                 .map(lambda x: x.split("#"))
products_rdd = products_rdd.map(lambda x:
                                Row(
                                    product_id=str(x[0]),
                                    product_name=str(x[1]),
                                    unit_price=float(x[2]),
                                    quantity=float(x[3])
                                ))
products_df = spark.createDataFrame(products_rdd)
print(products_df.show(10))

# Using either DataFrame methods or plain SQL statements find 5 customers with the largest spent on the day.
transactions_df.createOrReplaceTempView("transactions")
products_df.createOrReplaceTempView("products")
top_5_customers = spark.sql(
                    """
                        SELECT
                        customer_id,
                        SUM(quantity_bought) * SUM(price_paid) net_rev
                        FROM transactions
                        GROUP BY customer_id
                        ORDER BY net_rev DESC
                        LIMIT 5
                    """)
print(top_5_customers.show())


# Find the names of the products each of those 5 customers bought.
top_5_customer_products_bought = top_5_customers.join(transactions_df, "customer_id", "left")\
                                                .select(["customer_id", "product_id"])\
                                                .join(products_df, "product_id", "left")\
                                                .select(["customer_id", "product_id", "product_name"])
print(top_5_customer_products_bought.show())
print(top_5_customer_products_bought.count())



## Find the names and total number sold of 10 most popular products.
top_10_products = spark.sql(
                            """
                                SELECT
                                trans.product_id,
                                SUM(trans.quantity_bought) sum_qty_bought
                                FROM transactions AS trans
                                GROUP BY trans.product_id
                                ORDER BY sum_qty_bought DESC
                                LIMIT 10
                            """)
print(top_10_products.show())

top_10_products_df = top_10_products.join(products_df, top_10_products.product_id == products_df.product_id)\
                                    .select(["product_name", "sum_qty_bought"])\
                                    .orderBy(col('sum_qty_bought').desc())
print(top_10_products_df.show())
print(top_10_products_df.count())

## Order products once per the number sold and then by the total value (quanity*price) sold.
all_table = products_df.join(transactions_df, "product_id")
all_table = all_table.withColumn('sum_qty_bought', all_table.quantity_bought * all_table.price_paid)

all_table_order_by_quantity_bought = all_table.orderBy(col('quantity_bought').desc())
print(all_table_order_by_quantity_bought.show())

all_table_order_by_sum_qty_bought = all_table.orderBy(col('sum_qty_bought').desc())
print(all_table_order_by_sum_qty_bought.show())