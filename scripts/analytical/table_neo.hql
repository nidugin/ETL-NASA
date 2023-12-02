use ${hiveconf:database};
create external table if not exists ${hiveconf:table}(
    id int,
    `date` date,
    neo_reference_id int,
    name string,
    nasa_jpl_url string,
    absolute_magnitude_h float,
    is_potentially_hazardous_asteroid boolean,
    is_sentry_object boolean,
    PRIMARY KEY (id, `date`) DISABLE NOVALIDATE)
    partitioned by (year INT, month INT, day INT)
    LOCATION '${hiveconf:location}';