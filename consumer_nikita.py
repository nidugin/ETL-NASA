"""
consumer_nikita.py
Consumer for fetching data
from specific NASA topic.

Created by Nikita Roldugins 20/06/2023.
"""

import sys
from utils.time import *
from configparser import ConfigParser
from pyspark.sql import SparkSession


config = ConfigParser()
config.read("/home/airflow_usr/scripts/nikita/config.ini")

env_name = sys.argv[1]
api_name = sys.argv[2]

kafka_ip = config[env_name]["kafka_ip"]
topic_name = config[env_name][f"topic_{api_name}"]

landing_path = config[env_name][f"landing_{api_name}"]
ingest_landing_path = landing_path + f"/ingest_{year}-{month}-{day}"
checkpoint_path = config[env_name][f"checkpoint_{api_name}"]


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Consumer") \
        .master("local[*]") \
        .getOrCreate()

    # Turn off logs in terminal
    spark.sparkContext.setLogLevel("ERROR")

    # Read data from topics
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_ip) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

    df = kafka_df.selectExpr("CAST(value AS STRING)")
    output = df.writeStream.outputMode("append")\
        .format("json")\
        .option("checkpointLocation", checkpoint_path)\
        .option("path", ingest_landing_path)\
        .start()
    output.awaitTermination(300)

    spark.stop()
    print("Consumer is successfully completed")
