from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("readcsvdata").getOrCreate()

df = spark.read.csv("/winequality-red.csv")

print(type(df))

df.printSchema()
df.show()