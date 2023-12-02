use ${hiveconf:database};
create external table if not exists ${hiveconf:table}(
    `date` string,
    explanation string,
    hdurl string,
    media_type string,
    service_version string,
    title string,
    url string,
    file_name string,
    hd_file_name string)
    partitioned by (year INT, month INT, day INT)
    LOCATION '${hiveconf:location}';