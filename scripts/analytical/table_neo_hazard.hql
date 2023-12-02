use ${hiveconf:database};
create external table ${hiveconf:table}(
    neo_reference_id int,
    uuid string,
    `date` date,
    hazardous boolean,
    is_sentry_object boolean,
    PRIMARY KEY (neo_reference_id, `date`) DISABLE NOVALIDATE)
    partitioned by (year INT, month INT, day INT)
    LOCATION '${hiveconf:location}';