from pyspark.sql import SparkSession

sc = SparkSession.builder.appName("readdatajson").getOrCreate()
dataframe = sc.read.json("/demo1.json")
print(type(dataframe))

dataframe.show()


