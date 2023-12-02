use ${hiveconf:database};
create external table if not exists ${hiveconf:table}(
    id string,
    `date` string,
    neo_reference_id string,
    name string,
    nasa_jpl_url string,
    absolute_magnitude_h string,
    is_potentially_hazardous_asteroid string,
    is_sentry_object string)
    partitioned by (year INT, month INT, day INT)
    LOCATION '${hiveconf:location}';