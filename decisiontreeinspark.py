from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StandardScaler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer

spark = SparkSession\
.builder\
.appName("decisionTreeWithSpark").\
getOrCreate()

df = spark.read.csv("/winequality-red.csv",header=True)

# df.show()

from pyspark.sql.functions import col 
new_df = df.select(*(col(c).cast("float").alias (c) for c in df.columns))

# new_df.printSchema()

from pyspark.sql.functions import count,isnan,when

# new_df.select([count(when(col(c).isNull(),c)).alias (c) for c in df.columns]).show()

cols = new_df.columns
cols.remove("quality")

assembler = VectorAssembler(inputCols=cols,outputCol="features")

data = assembler.transform(new_df)
data.select("features","quality").show()
# data.show()

from pyspark.ml.feature import StringIndexer

stringIndexer = StringIndexer(inputCol="quality",outputCol="quality_index")
data_indexed = stringIndexer.fit(data).transform(data)
data_indexed.show()

# split the data
(train,test) = data_indexed.randomSplit([0.7,0.3])

# model Building

dt = DecisionTreeClassifier(labelCol="quality_index",featuresCol="features")
model = dt.fit(train)

prediction = model.transform(test)

prediction.show()

# evalutor 
evaluator = MulticlassClassificationEvaluator(labelCol="quality_index",predictionCol="prediction",metricName="accuracy")
accuracy = evaluator.evalute(prediction)

print("Accuracy: ", accuracy)

# saving model 
model.save("decisiontree.pkl")
