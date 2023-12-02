"""
transformations.py
Transformations for the Analytical Layer

Created by Nikita Roldugins 07/17/2023.
"""


import uuid
import hashlib
from pyspark.sql.functions import (
    col,
    min,
    max,
    avg,
    lit,
    udf,
    desc,
    size,
    split,
    upper,
    round,
    to_date,
    element_at
)


def apod(df):
    final_df = df.select(
        col("date").cast("date"),
        col("explanation"),
        col("hdurl"),
        col("media_type"),
        col("service_version"),
        upper(col("title")).alias("title"),
        col("url"),
        element_at(split(col("file_name"), "/"), -1).alias("file_name"),
        element_at(split(col("hd_file_name"), "/"), -1).alias("hd_file_name"),
        col("year"),
        col("month"),
        col("day")
    )
    return final_df


def v_apod_dashboard_las_5_days(df):
    sorted_df = df.select(
        col("explanation"),
        col("title"),
        col("date")
    ).sort(col("date").cast("date").desc()).limit(5)

    edited_df = sorted_df\
        .withColumn("timeframe", lit("last_5_days"))\
        .withColumn("explanation_count", size(split(col("explanation"), " ")))\
        .withColumn("title_count", size(split(col("title"), " ")))

    edited_df = edited_df.drop("date")
    final_df = edited_df.groupBy('timeframe').agg(
        min("explanation_count").alias("explanation_count_min"),
        min("title_count").alias("title_count_min"),
        max("explanation_count").alias("explanation_count_max"),
        max("title_count").alias("title_count_max"),
        avg("explanation_count").cast("int").alias("explanation_count_average"),
        avg("title_count").cast("int").alias("title_count_average")
    )

    return final_df


def neo(df):
    final_df = df.select(
        col("id").cast("int"),
        to_date("date", "yyyy-MM-dd").alias("date"),
        col("neo_reference_id").cast("int"),
        col("name"),
        col("nasa_jpl_url"),
        round(col("absolute_magnitude_h").cast("float"), 1).alias("absolute_magnitude_h"),
        col("is_potentially_hazardous_asteroid").cast("boolean"),
        col("is_sentry_object").cast("boolean"),
        col("year"),
        col("month"),
        col("day")
    )
    return final_df


def neo_hazard(df):
    # Convert value to the UUID
    convert_uuid = udf(lambda x: str(uuid.UUID(hashlib.md5(x.encode()).hexdigest())), "string")

    selected_df = df.select(
        col("neo_reference_id").cast("int"),
        convert_uuid(col("neo_reference_id")).alias("uuid"),
        to_date("date", "yyyy-MM-dd").alias("date"),
        col("is_potentially_hazardous_asteroid").cast("boolean").alias("hazardous"),
        col("is_sentry_object").cast("boolean").alias("is_sentry_object"),
        col("year"),
        col("month"),
        col("day")
    )
    final_df = selected_df.filter(col("hazardous") == True)
    return final_df


def v_neo_hazard_dashboard_5(df):
    # Convert value to the UUID
    convert_uuid = udf(lambda x: str(uuid.UUID(hashlib.md5(x.encode()).hexdigest())), "string")

    selected_df = df.select(
        col("neo_reference_id").cast("int"),
        convert_uuid(col("neo_reference_id")).alias("uuid"),
        col("is_potentially_hazardous_asteroid").cast("boolean").alias("hazardous"),
    )
    final_df = selected_df.filter(col("hazardous") == True)
    return final_df
