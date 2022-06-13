{%- set price_divisor = 1000000 -%} -- ga money values are x10^6

with

sessions as (
	select * from {{ ref('stg_ga__sessions') }} as s
    where s.product_sku != '(not set)'
),

sessions_this_week as (
    select * from {{ ref('stg_ga__sessions') }} as s
    where
        (s.product_sku != '(not set)') and
        (s.date between '2022-06-05' and '2022-06-11')
),

pivot_and_aggregate_sessions_to_product_level as (
	select
		product_name,
		product_sku,
        sum(product_revenue/{{price_divisor}}) as revenue,
        count(distinct if(action_type = 'view', client_id, null)) as num_users_viewed,
        countif(action_type = 'view') as num_views,
        countif(action_type = 'purchase') as purchases,
        (countif(action_type = 'purchase') / nullif(countif(action_type = 'view'), 0)) as conversion_rate
	from sessions
    {{ group_by_first(2) }}
    order by 2
)

select * from pivot_and_aggregate_sessions_to_product_level
