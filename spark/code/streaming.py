from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.dataframe import DataFrame
from pyspark.conf import SparkConf
from pyspark.ml.pipeline import PipelineModel
import pyspark.sql.types as tp
from pyspark.sql.functions import from_json
import base64
from pyspark.sql.functions import udf, concat_ws, from_unixtime, date_format
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from PIL import Image
import io
import categories


def process(batch_df: DataFrame, batch_id: int):
    print("Processing batch: ", batch_id)
    df = batch_df
    df = df.withColumn(
        "ground_truth_label",
        udf(
            lambda subreddit: categories.subreddit_categories.get(subreddit, 3),
            tp.IntegerType(),
        )(col("subreddit")),
    )
    # df = df.withColumn("label", df["label"].cast(tp.DoubleType()))

    df = df.withColumn(
        "created_utc", from_unixtime(col("created_utc")).cast("timestamp")
    )
    # https://github.com/elastic/elasticsearch-hadoop/issues/1173
    df = df.withColumn(
        "created_timestamp",
        date_format(col("created_utc"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
    )

    df = df.withColumn(
        "all_text", concat_ws(" ", df.title, df.selftext, df.ocr_text, df.caption_text)
    )

    df1 = classificationModel.transform(df)
    df1 = df1.withColumn("pred_int", df1["prediction"].cast(tp.IntegerType()))
    df1 = df1.withColumn(
        "predicted_category",
        udf(
            lambda cat_id: categories.categories_names.get(cat_id, "oth"),
            tp.StringType(),
        )(col("pred_int")),
    )
    df1 = df1.withColumn(
        "ground_truth_category",
        udf(
            lambda cat_id: categories.categories_names.get(cat_id, "oth"),
            tp.StringType(),
        )(col("ground_truth_label")),
    )

    df1 = df1.select(
        "id",
        "created_timestamp",
        "title",
        "selftext",
        "caption_text",
        "ocr_text",
        "score",
        "upvote_ratio",
        "subreddit",
        "img_url",
        "num_comments",
        "img_filename",
        "predicted_category",
        "ground_truth_category",
        "all_text",
    )
    df1.show()
    print("Processed batch: ", batch_id, "writing to elastic")
    df1.write.format("es").mode("append").option("es.mapping.id", "id").save(
        elastic_index
    )


kafkaServer = "kafkaServer:9092"
topic = "reddit-posts"
modelPath = "/opt/tap/models/ovrModel"
elastic_index = "reddit-posts"

sparkConf = SparkConf().set("es.nodes", "elasticsearch").set("es.port", "9200")
spark = (
    SparkSession.builder.appName("reddit-streaming")
    .config(conf=sparkConf)
    .getOrCreate()
)

# To reduce verbose output
spark.sparkContext.setLogLevel("ERROR")


""" posts_schema = tp.StructType(
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
    ]
) """

document_schema = tp.StructType(
    [
        tp.StructField(
            "document",
            tp.StructType(
                [
                    tp.StructField("author", tp.StringType(), True),
                    tp.StructField("ocr_text", tp.StringType(), True),
                    tp.StructField("upvote_ratio", tp.DoubleType(), True),
                    tp.StructField("created_utc", tp.DoubleType(), True),
                    tp.StructField("subreddit", tp.StringType(), True),
                    tp.StructField("img_url", tp.StringType(), True),
                    tp.StructField("num_comments", tp.IntegerType(), True),
                    tp.StructField("id", tp.StringType(), True),
                    tp.StructField("title", tp.StringType(), True),
                    tp.StructField("score", tp.IntegerType(), True),
                    tp.StructField("img_filename", tp.StringType(), True),
                    tp.StructField("selftext", tp.StringType(), True),
                    tp.StructField("caption_text", tp.StringType(), True),
                ]
            ),
        )
    ]
)


print("Reading stream from kafka...")
# Read the stream from kafka
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafkaServer)
    .option("subscribe", topic)
    .load()
)

# Cast the message received from kafka with the provided schema
# df.printSchema()
""" |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true) """
kafka_df = kafka_df.selectExpr(
    "CAST(timestamp AS STRING)", "CAST(value AS STRING)"
).select("timestamp", from_json("value", document_schema).alias("data"))
# kafka_df.printSchema()

df = kafka_df.select("data.document.*")


print("Loading model")
classificationModel = PipelineModel.load(modelPath)
print("Done")

# df.printSchema()
""" root
spark  |  |-- author: string (nullable = true)
spark  |  |-- ocr_text: string (nullable = true)
spark  |  |-- upvote_ratio: double (nullable = true)
spark  |  |-- created_utc: double (nullable = true)
spark  |  |-- subreddit: string (nullable = true)
spark  |  |-- img_url: string (nullable = true)
spark  |  |-- num_comments: integer (nullable = true)
spark  |  |-- id: string (nullable = true)
spark  |  |-- title: string (nullable = true)
spark  |  |-- score: integer (nullable = true)
spark  |  |-- img_filename: string (nullable = true)
spark  |  |-- selftext: string (nullable = true) """


""" df1.writeStream.option("checkpointLocation", "/tmp/").format("es").start(
    elastic_index
).awaitTermination() """
# df1.writeStream.format("console").option("truncate", "true").start().awaitTermination()
df.writeStream.foreachBatch(process).start().awaitTermination()
