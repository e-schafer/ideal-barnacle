"""
This script is a simple example of how to consume data from kafka using spark
"""
import json

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F


def sink_selector(data: Row):
    """

    :param data: Row
    :return: dict
    """

    stuff = json.loads(data.asDict()["value"])
    print("new item")
    print(stuff)
    return stuff


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
    spark.sparkContext.setLogLevel("ERROR")

    # read a stream from kafka
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "test")
        .option("startingOffsets", "earliest")
        .load()
    )

    ds = (
        df.select(F.col("key").cast("String"), F.col("value").cast("String"))
        .writeStream.format("console")
        .foreach(sink_selector)
        .start()
    )

    ds.processAllAvailable()
