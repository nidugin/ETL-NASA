use ${hiveconf:database};
create external table if not exists ${hiveconf:table}(
    timeframe string,
    explanation_count_min int,
    title_count_min int,
    explanation_count_max int,
    title_count_max int,
    explanation_count_average int,
    title_count_average int,
    PRIMARY KEY (timeframe) DISABLE NOVALIDATE)
    LOCATION '${hiveconf:location}';