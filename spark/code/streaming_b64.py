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
from pyspark.sql.functions import udf, concat_ws
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from PIL import Image
import io


kafkaServer = "kafkaServer:9092"
topic = "reddit-posts"
modelPath = "./models/ovr_model"
elastic_index = "reddit-posts"

sparkConf = SparkConf().set("es.nodes", "elasticsearch").set("es.port", "9200")
spark = (
    SparkSession.builder.appName("reddit-streaming")
    .config(conf=sparkConf)
    .getOrCreate()
)

# To reduce verbose output
spark.sparkContext.setLogLevel("ERROR")

""" print("Loading model")
sentitapModel = PipelineModel.load(modelPath)
print("Done")
 """
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
                    tp.StructField("image_base64", tp.StringType(), True),
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
kafka_df.printSchema()

df = kafka_df.select("data.document.*")
df.printSchema()
""" root
spark  |  |-- author: string (nullable = true)
spark  |  |-- ocr_text: string (nullable = true)
spark  |  |-- image_base64: string (nullable = true)
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

img_df = (
    spark.readStream.format("image")
    .option("dropInvalid", value=True)
    .load("/opt/tap/code/images")
)

img_df.writeStream.format("console").option(
    "truncate", "true"
).start().awaitTermination()

""" def decode_base64(image_base64):
    return base64.b64decode(image_base64)


decode_base64_udf = udf(decode_base64, tp.BinaryType())
df = df.withColumn("image_binary", decode_base64_udf(col("image_base64")))


# https://github.com/microsoft/spark-images/blob/master/src/main/scala/org/apache/spark/image/ImageSchema.scala
def create_image_schema(binary_data):
    image = Image.open(io.BytesIO(binary_data))
    width, height = image.size
    nChannels = len(image.getbands())
    mode = image.mode
    return (None, height, width, nChannels, mode, binary_data)


create_image_schema_udf = udf(create_image_schema)

df = df.withColumn("image", create_image_schema_udf(col("image_binary")))
 """

imageAssembler = ImageAssembler().setInputCol("image").setOutputCol("image_assembler")
imageCaptioning = (
    VisionEncoderDecoderForImageCaptioning.pretrained()
    .setBeamSize(2)
    .setDoSample(False)
    .setInputCols(["image_assembler"])
    .setOutputCol("caption")
)
IMGpipeline = Pipeline().setStages([imageAssembler, imageCaptioning])
IMGdf = IMGpipeline.fit(df).transform(df)
""" df.selectExpr(
    "reverse(split(image.origin, '/'))[0] as image_name", "caption.result"
).show(truncate=False) """
IMGdf = IMGdf.withColumn("caption_text", concat_ws(" ", "caption.result"))

# df.writeStream.format("console").option("truncate", "false").start().awaitTermination()
IMGdf.select(
    "id",
    "title",
    "subreddit",
    "score",
    "num_comments",
    "created_utc",
    "author",
    "upvote_ratio",
    "img_filename",
    "ocr_text",
    "url",
    "selftext",
    "img_url",
    "caption_text",
).writeStream.format("console").option("truncate", "false").start().awaitTermination()

""" df.writeStream.format("console").option(
    "truncate", "true"
).start().awaitTermination() """

""" df1.writeStream.option("checkpointLocation", "/tmp/").format("es").start(
    elastic_index
).awaitTermination() """
