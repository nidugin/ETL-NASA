"""
functions.py
Functions that
are mainly used for the
Analytical Layer and Base Layer

Created by Nikita Roldugins 07/13/2023.
"""

from utils.time import *
from pyspark.sql.functions import (
    col,
    count,
    expr,
    lit,
    concat_ws
)


def get_api_name(table_name):
    """
    Get name of the api from the name of the table
    :param table_name: name for casting
    :return: name of the api
    """
    if "apod" in table_name:
        return "apod"
    elif "neo" in table_name:
        return "neo"
    else:
        raise Exception("Can't find api_name in the table_name")


def get_column_list(table_name):
    """
    Get mandatory column list for specific table
    :param table_name: table for search
    :return: mandatory column list
    """
    column_dictionary = {
        "apod": ["date"],
        "v_apod_dashboard_las_5_days": [
            "explanation",
            "title"
        ],
        "neo": [
            "id",
            "date",
            "name"
        ],
        "neo_hazard": [
            "neo_reference_id",
            "date",
            "is_potentially_hazardous_asteroid",
            "is_sentry_object"
        ],
        "v_neo_hazard_dashboard_5": [
            "neo_reference_id",
            "is_potentially_hazardous_asteroid"
        ]
    }
    return column_dictionary[table_name]


def get_partitions(table_name):
    """
    Returns list of the partitions for specific table
    :param table_name: find partitions for the table
    :return: list of the partitions
    """
    error_table = "error_log"
    dashboard_table = ["v_apod_dashboard_las_5_days", "v_neo_hazard_dashboard_5"]
    if table_name in error_table:
        return ["data_asset", "table_name", "year", "month", "day"]
    elif table_name in dashboard_table:
        return None
    else:
        return ["year", "month", "day"]


def get_mode(table_name):
    """
    Returns append/overwrite for the write_table according to the name of the table
    :param table_name: find mode for the table
    :return: overwrite/append
    """
    tables = "v_apod_dashboard_las_5_days" or "v_neo_hazard_dashboard_5"
    if tables == table_name:
        return "overwrite"
    else:
        return "append"


def write_table(df, partition_list, mode, table_path, table_name):
    """
    Writes df to the specific table with different parameters
    :param df: the df that has to be written in a table
    :param partition_list: split data by partitions
    :param mode: how to write data to the table
    :param table_path: path to save table
    :param table_name: name
    """
    if partition_list is None:
        df.write.mode(mode) \
            .format("hive") \
            .option("path", table_path) \
            .saveAsTable(table_name)
    else:
        df.show()
        df.write.partitionBy(partition_list) \
            .mode(mode) \
            .format("hive") \
            .option("path", table_path) \
            .saveAsTable(table_name)

    print("Data successfully written.")
    print(f"Rows added: {df.count()}.")


def check_duplicates(df, data_asset, api_name):
    """
    Filters if the df has duplicates and writes them in error_df
    :param df: df that has to be checked for duplicates
    :param data_asset: data_asset name for column
    :param api_name: name of the api
    :return final_df: clean df
    :return error_df: error df
    """
    final_df = df.distinct()
    error_df = df.exceptAll(final_df)
    error_df = error_df.select(
        lit(data_asset).alias("data_asset"),
        lit(api_name + "_base").alias("table_name"),
        lit("duplicate").alias("error_type"),
        concat_ws(";", *error_df.columns).alias("original_record")
    )
    error_df = error_df \
        .withColumn("year", lit(year)) \
        .withColumn("month", lit(month)) \
        .withColumn("day", lit(day))

    return final_df, error_df


def check_mandatory_columns(df, columns, data_asset, api_name):
    """
    Filters df that doesn't have mandatory columns and writes them in error_df
    :param df: df that has to be checked for needed columns
    :param columns: list of the mandatory columns
    :param data_asset: data_asset name for column
    :param api_name: name of the api
    :return final_df: clean df
    :return error_df: error df
    """
    error_df = df.filter(expr(" OR ".join("{0} IS NULL".format(col_name) for col_name in columns)))
    final_df = df.exceptAll(error_df)
    error_df = error_df.select(
        lit(data_asset).alias("data_asset"),
        lit(api_name + "_base").alias("table_name"),
        lit("duplicate").alias("error_type"),
        concat_ws(";", *error_df.columns).alias("original_record")
    )
    error_df = error_df \
        .withColumn("year", lit(year)) \
        .withColumn("month", lit(month)) \
        .withColumn("day", lit(day))

    return final_df, error_df
