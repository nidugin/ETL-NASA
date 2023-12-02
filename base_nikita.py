"""
base_nikita.py
Preparing "raw" data from Landing Layer
for Analytical Layer transformations.

Created by Nikita Roldugins 27/06/2023.
"""


import sys
from utils.time import *
from utils.functions import write_table
from configparser import ConfigParser
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    explode,
    lit,
    col
)
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType
)


config = ConfigParser()
config.read("/home/airflow_usr/scripts/nikita/config.ini")

env_name = sys.argv[1]
api_name = sys.argv[2]

base_table = config[env_name]["name"] + "_base_" + env_name + "." + api_name

base_path = config[env_name][f"base_{api_name}"]
landing_path = config[env_name][f"landing_{api_name}"]
ingest_landing_path = landing_path + f"/ingest_{year}-{month}-{day}"


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("Base") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    # Turn off logs in terminal
    spark.sparkContext.setLogLevel("ERROR")

    if api_name == "apod":
        schema = StructType([
            StructField("date", StringType()),
            StructField("explanation", StringType()),
            StructField("hdurl", StringType()),
            StructField("media_type", StringType()),
            StructField("service_version", StringType()),
            StructField("title", StringType()),
            StructField("url", StringType()),
        ])

        # Read JSON file and apply schema
        df = spark.read \
            .option("multiline", "true") \
            .json(ingest_landing_path)
        mapped_df = df.withColumn("value", from_json("value", schema))
        # Get required data to columns
        sorted_df = mapped_df \
            .withColumn("date", col("value.date")) \
            .withColumn("explanation", col("value.explanation")) \
            .withColumn("hdurl", col("value.hdurl")) \
            .withColumn("media_type", col("value.media_type")) \
            .withColumn("service_version", col("value.service_version")) \
            .withColumn("title", col("value.title")) \
            .withColumn("url", col("value.url")) \
            .withColumn("file_name", col("value.url")) \
            .withColumn("hd_file_name", col("value.hdurl"))
        sorted_df = sorted_df.drop("value", "neb_explode", "cad_explode")
    elif api_name == "neo":
        schema = StructType([
            StructField("links", StringType()),
            StructField("element_count", StringType()),
            StructField("near_earth_objects", ArrayType(
                StructType([
                    StructField("id", StringType()),
                    StructField("neo_reference_id", StringType()),
                    StructField("close_approach_data", ArrayType(
                        StructType([
                            StructField("close_approach_date", StringType()),
                        ]))),
                    StructField("name", StringType()),
                    StructField("nasa_jpl_url", StringType()),
                    StructField("absolute_magnitude_h", StringType()),
                    StructField("is_potentially_hazardous_asteroid", StringType()),
                    StructField("is_sentry_object", StringType())
                ])))
        ])

        # Read JSON file and apply schema
        df = spark.read \
            .option("multiline", "true") \
            .json(ingest_landing_path)
        mapped_df = df.withColumn("value", from_json("value", schema))

        # Get required data to columns
        sorted_df = mapped_df \
            .withColumn("neb_explode", explode("value.near_earth_objects")) \
            .withColumn("cad_explode", explode("neb_explode.close_approach_data")) \
            .withColumn("id", col("neb_explode.id")) \
            .withColumn("date", col("cad_explode.close_approach_date")) \
            .withColumn("neo_reference_id", col("neb_explode.neo_reference_id")) \
            .withColumn("name", col("neb_explode.name")) \
            .withColumn("nasa_jpl_url", col("neb_explode.nasa_jpl_url")) \
            .withColumn("absolute_magnitude_h", col("neb_explode.absolute_magnitude_h")) \
            .withColumn("is_potentially_hazardous_asteroid", col("neb_explode.is_potentially_hazardous_asteroid")) \
            .withColumn("is_sentry_object", col("neb_explode.is_sentry_object"))
        sorted_df = sorted_df.drop("value", "neb_explode", "cad_explode")
    else:
        raise Exception("API name is not provided")

    # Add date columns for further partitioning
    final_df = sorted_df \
        .withColumn("year", lit(year).cast("int")) \
        .withColumn("month", lit(month).cast("int")) \
        .withColumn("day", lit(day).cast("int"))

    # Write dataframe to hive table
    partition_list = ["year", "month", "day"]
    write_table(final_df, partition_list, "append", base_path, base_table)

    spark.stop()
