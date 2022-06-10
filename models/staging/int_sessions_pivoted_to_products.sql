{%- set price_divisor = 1000000 -%}

with

sessions as (
	select * from {{ ref('stg_ga__sessions') }}
),

sessions_this_week as (
    select * from {{ ref('stg_ga__sessions') }} as s 
    where s.date between '2022-06-05' and '2022-06-11'
),

pivot_and_aggregate_sessions_to_product_level as (
	select
		product_sku,
		product_name,
        sum(product_revenue/{{price_divisor}}) as revenue,
        
	from sessions
    {{ group_by_first(2) }}
    order by 1
)

select * from pivot_and_aggregate_sessions_to_product_level
