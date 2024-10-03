# test_job.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestJob").getOrCreate()
df = spark.range(100)
df.show()
