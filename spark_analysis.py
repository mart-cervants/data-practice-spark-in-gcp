from pyspark.sql import SparkSession
import pyspark.sql.functions as sqlf
from datetime import datetime

DATADIR = "gs://mcerv-spark-with-dataproc/data/"
CUSTOMER_SAMPLE = 0.5
print("User Sample Size: ", CUSTOMER_SAMPLE)

spark_session = SparkSession.builder.getOrCreate()

customers = spark_session.read.option("header", True).option("inferSchema", True).csv(DATADIR + "customers100000.csv")
customers.printSchema()
customers.show()
num_customers = customers.count()

products = spark_session.read.option("header", True).option("inferSchema", True).csv(DATADIR + "products10000.csv")
products.printSchema()
products.show(5)
num_products = products.count()

# # Common Task: Do a crossJoin

# How many rows to expect?
# num_customers * num_products * 2 columns * 8byte precision / 10**9 = memory footprint in GB
print("Expected in-memory footprint in GB: ", CUSTOMER_SAMPLE * num_customers * num_products * 2 * 8 / 10**9)

# Do the cross join. .repartition is crucial here 
# At the end you when you have overwrite your data you can check your buckets
# to see if the number of partitions correspond to (100*CUSTOMER_SAMPLE)
transactions = customers.sample(CUSTOMER_SAMPLE).repartition(int(100*CUSTOMER_SAMPLE)).crossJoin(products)
transactions.createOrReplaceTempView("transactions")

# Add a random date between January and December 2018
jan18 = datetime.strptime("2018-01-01", "%Y-%m-%d")
dec18 = datetime.strptime("2018-12-31", "%Y-%m-%d")

print("Create artificial timestamp ...")
tmp = transactions.limit(1000)
tmp.withColumn("date", (jan18.timestamp() + sqlf.rand() * (dec18.timestamp()-jan18.timestamp())).cast("timestamp")).show()

transactions_w_date = transactions.withColumn("date", (jan18.timestamp() + sqlf.rand() * (dec18.timestamp()-jan18.timestamp())).cast("timestamp"))

print("Writing to Parquet")
transactions_w_date.select(["cust_id", "product_id", "date"]).write.mode("overwrite").parquet(DATADIR + "transactions_sample_{}_pq".format(CUSTOMER_SAMPLE))

print("Writing to CSV")
spark_session.read.parquet(DATADIR + "transactions_sample_{}_pq".format(CUSTOMER_SAMPLE)).write.option("header", True).csv(
    DATADIR + "transactions_sample_{}_csv".format(CUSTOMER_SAMPLE)
    )