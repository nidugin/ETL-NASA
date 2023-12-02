use ${hiveconf:database};
create external table if not exists ${hiveconf:table}(
    error_type string,
    original_record string)
    partitioned by (data_asset STRING, table_name STRING, year INT, month INT, day INT)
    LOCATION '${hiveconf:location}';