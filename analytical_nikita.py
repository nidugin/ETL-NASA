"""
analytical_nikita.py
Preparing "raw" data from Landing Layer
for Analytical Layer transformations.

Created by Nikita Roldugins 07/13/2023.
"""


import sys
import utils.transformations as tr
from configparser import ConfigParser
from pyspark.sql import SparkSession
from utils.time import *
from utils.functions import (
    get_mode,
    write_table,
    get_api_name,
    get_partitions,
    get_column_list,
    check_duplicates,
    check_mandatory_columns
)

config = ConfigParser()
config.read("/home/airflow_usr/scripts/nikita/config.ini")

env_name = sys.argv[1]
table_name = sys.argv[2]

error_table_name = "error_log"

my_name = config[env_name]["name"]
api_name = get_api_name(table_name)
data_asset_name = api_name + "_" + my_name

full_error_table = my_name + "_error_" + env_name + "." + error_table_name
full_base_table = my_name + "_base_" + env_name + "." + api_name
full_analytical_table = my_name + "_analytical_" + env_name + "." + table_name

analytical_path = config[env_name][f"analytical_{api_name}"]
error_path = config[env_name]["error_common"]

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Analytical") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    # Turn off logs in terminal
    spark.sparkContext.setLogLevel("ERROR")

    # Read from base table to df
    df = spark.table(full_base_table)
    # Find rows that don't suit mandatory columns (null values) and write them into the error table
    cleaned_df, error_df = check_mandatory_columns(df, get_column_list(table_name), data_asset_name, api_name)
    write_table(error_df, get_partitions(error_table_name), get_mode(error_table_name), error_path, full_error_table)

    # Find duplicated rows and write them into the error table
    distinct_df, error_df = check_duplicates(cleaned_df, data_asset_name, api_name)
    write_table(error_df, get_partitions(error_table_name), get_mode(error_table_name), error_path, full_error_table)

    # Make transformed_df from clean_df
    transformed_df = eval(f"tr.{table_name}(distinct_df)")
    # # Write transformed_df to the valid (output) table
    write_table(transformed_df, get_partitions(table_name), get_mode(table_name), analytical_path, full_analytical_table)

    spark.stop()
