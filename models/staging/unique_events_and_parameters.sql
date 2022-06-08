-- this query creates a list of unique events and parameters with the corresponding data types and the amount these event-parameter combinations occur in the GA4 property (only within the lookback period)
SELECT
  event_name,
  params.key AS event_parameter_key,
  CASE
    WHEN params.value.string_value IS NOT NULL THEN 'string'
    WHEN params.value.int_value IS NOT NULL THEN 'int'
    WHEN params.value.double_value IS NOT NULL THEN 'double'
    WHEN params.value.float_value IS NOT NULL THEN 'float'
END
  AS event_parameter_value,
  count(*) as amount
FROM
  {{ ref('dedup_events') }},
  UNNEST(event_params) AS params
WHERE
	table_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('session_lookback_days') }} DAY) AND CURRENT_DATE()
GROUP BY
  event_name,
  event_parameter_key,
  event_parameter_value
ORDER BY
  event_name,
  event_parameter_key