from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .appName("reading from socket") \
    .master("local[*]") \
    .getOrCreate()

df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "9999") \
    .load()

df.printSchema()

df = df.withColumn("words", F.split(F.col("value"), " ")) \
    .withColumn("word", F.explode(F.col("words"))) \
    .drop("value","words")

df.groupBy(F.col("word")) \
    .agg(F.count(F.lit(1)).alias("cnt")) \
    .writeStream \
    .format("console") \
    .outputMode("complete") \
    .start() \
    .awaitTermination()

