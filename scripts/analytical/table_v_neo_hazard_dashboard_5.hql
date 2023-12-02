use ${hiveconf:database};
create external table if not exists ${hiveconf:table}(
    neo_reference_id int,
    uuid string,
    hazardous boolean,
    PRIMARY KEY (neo_reference_id) DISABLE NOVALIDATE)
    LOCATION '${hiveconf:location}';