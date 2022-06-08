{{ config(
    materialized='incremental',
    partition_by={
      "field": "table_date",
      "data_type": "date"},
    cluster_by= ["table_date","user_pseudo_id","event_name"],
    incremental_strategy = 'insert_overwrite'
)}}

select
  * except(row)
from (
  select
    -- extracts date from source table
    parse_date('%Y%m%d',regexp_extract(_table_suffix,'[0-9]+')) as table_date,
    -- flag to indicate if source table is `events_intraday_`
    case when _table_suffix like '%intraday%' then true else false end as is_intraday,
    *,
    row_number() over (partition by user_pseudo_id, event_name, event_timestamp order by event_timestamp) as row
  from
    `{{ target.project }}.{{ target.schema }}.events_*`
    
  {% if is_incremental() %}
  -- Refresh only recent session data to limit query costs, unless running with --full-refresh
	where regexp_extract(_table_suffix,'[0-9]+') BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('session_lookback_days') }} DAY)) AND
  		FORMAT_DATE("%Y%m%d", CURRENT_DATE())
{% endif %}  
    )
where
  row = 1