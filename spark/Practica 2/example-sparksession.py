from pyspark.sql import SparkSession, SQLContext

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

df = SQLContext().