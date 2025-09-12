from pyspark.sql import SparkSession
import time

spark = SparkSession \
    .builder \
    .appName('simple_pyspark') \
    .getOrCreate()

schema = 'NAME STRING, AGE INT, MARRIAGE BOOLEAN'
df = spark.createDataFrame(data=[('hong', 28, False), ('kim', 30, True)], schema=schema)
df.show()

time.sleep(300)