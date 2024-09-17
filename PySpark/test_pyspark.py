from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.createDataFrame([(1, 'foo'), (2, 'bar')], ["id", "value"])
df.show()
