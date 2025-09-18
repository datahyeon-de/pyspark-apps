from pyspark.sql import SparkSession

app_name = 'kafka_streaming_base'
spark = SparkSession.builder.appName(app_name).getOrCreate()

kafka_read_option = {
    'kafka.bootstrap.servers': "kafka01:9092,kafka02:9092,kafka03:9092",
    'subscribe': 'lesson.spark-streaming.test',
}

kafka_source_df = spark.readStream.format('kafka').options(**kafka_read_option).load()

kafka_source_df = kafka_source_df.selectExpr(
    "CAST(key AS STRING) AS KEY",
    "CAST(value AS STRING) AS VALUE",
)

kafka_write_option = {
    'checkpointLocation': f'/home/spark/kafka_offsets/{app_name}',
    'truncate': 'false'
}

query = kafka_source_df.writeStream.format('console').options(**kafka_write_option).start()

query.awaitTermination()
