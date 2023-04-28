"""
This script is a simple example of how to consume data from kafka using spark
"""
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


schema = StructType(
    [
        StructField("dtype", StringType(), True),
        StructField("weight", IntegerType(), True),
        StructField("height", IntegerType(), True),
        StructField("age", IntegerType(), True),
        StructField("color", StringType(), True),
    ]
)


def parse_me(data: str):
    """
    This function parses the data from the kafka message
    """
    js_data = json.loads(data)
    main_key = str(list(js_data.keys())[0])
    return {**js_data[main_key], "dtype": main_key}


udf_read_my_json = F.udf(parse_me, schema)

if __name__ == "__main__":

    # create a spark session
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("Streaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("FATAL")

    # read a stream from kafka
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "test")
        .option("startingOffsets", "earliest")
        .load()
        .select(F.col("key").cast("String"), F.col("value").cast("String"))
    )

    ds = (
        df.select(
            F.col("value"),
            udf_read_my_json(F.col("value")).alias("substruct"),
        )
        .select(F.col("substruct.*"))
        .writeStream.option("truncate", False)
        .format("console")
        .start()
    )

    ds.processAllAvailable()
    ds.stop()
    spark.stop()
