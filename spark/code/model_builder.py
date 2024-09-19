from pyspark.ml.feature import SQLTransformer

# import findspark
import pyspark.sql.types as tp
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
import categories
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
import os
from pathlib import Path
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql.functions import concat_ws
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


DATASET_PATH = "/opt/dataset/dataset.csv"
IMAGES_PATH = "/opt/dataset/images/"

""" spark = (
    SparkSession.builder.appName("Training Model")
    # .master("local[4]")
    .config("spark.driver.memory", "16G")
    .config("spark.driver.maxResultSize", "0")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    # .config("spark.kryoserializer.buffer.max", "2000M")
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp-gpu_2.12:5.4.0")
    # .config("spark.local.dir", "C:/tmp")
    .getOrCreate()
) """

spark = SparkSession.builder.appName("Training Model").getOrCreate()
# To reduce verbose output
spark.sparkContext.setLogLevel("ERROR")

posts_schema = tp.StructType(
    [
        tp.StructField(name="id", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="title", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="subreddit", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="score", dataType=tp.IntegerType(), nullable=False),
        tp.StructField(name="num_comments", dataType=tp.IntegerType(), nullable=False),
        tp.StructField(name="created_utc", dataType=tp.FloatType(), nullable=True),
        tp.StructField(name="author", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="upvote_ratio", dataType=tp.FloatType(), nullable=False),
        tp.StructField(name="img_filename", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="ocr_text", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="url", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="selftext", dataType=tp.StringType(), nullable=True),
        tp.StructField(name="img_url", dataType=tp.StringType(), nullable=True),
        tp.StructField(name="caption_text", dataType=tp.StringType(), nullable=True),
    ]
)

print("Loading dataset...")

df = (
    spark.read.format("csv")
    .schema(posts_schema)
    #   .options(multiline=True, mode="FAILFAST", emptyValue="", nullValue="")
    .option("multiline", True)
    .option("mode", "FAILFAST")
    .option("emptyValue", "")
    .option("nullValue", "")
    .option("delimiter", ",")
    .option("quote", '"')
    .option("escape", '"')
    .load(DATASET_PATH, header=True)
)
print("Dataset loaded, # rows:", df.count())

df = df.withColumn("created_utc", from_unixtime(col("created_utc")).cast("timestamp"))

df = df.fillna({"selftext": ""})
df = df.withColumn(
    "label",
    udf(
        lambda subreddit: categories.subreddit_categories.get(subreddit, 3),
        tp.IntegerType(),
    )(col("subreddit")),
)


df = df.withColumn("label", df["label"].cast(tp.DoubleType()))
df = df.withColumn(
    "all_text", concat_ws(" ", df.title, df.selftext, df.ocr_text, df.caption_text)
)

tokenizer = Tokenizer(inputCol="all_text", outputCol="words")
# train_wordsData = tokenizer.transform(train)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=2**16)
# train_featurizedData = hashingTF.transform(train_wordsData)

idf = IDF(inputCol="rawFeatures", outputCol="features")
# idfModel = idf.fit(train_featurizedData)
# train_rescaledData = idfModel.transform(train_featurizedData)
lr = LogisticRegression(maxIter=10, tol=1e-6, fitIntercept=True)

ovr = OneVsRest(classifier=lr)


pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, ovr])
train, test = df.randomSplit([0.8, 0.2])

print("Training model...")
pipelineModel = pipeline.fit(train)
print("Done. Saving model...")
pipelineModel.write().overwrite().save("/opt/tap/models/ovrModel")
print("Model saved")
predictions = pipelineModel.transform(test)
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")

accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

spark.stop()
