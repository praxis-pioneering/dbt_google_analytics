{%- set price_divisor = 1000000 -%} -- ga money values are x10^6

with

{{ get_product_sessions() }},

pivot_and_aggregate_sessions_to_product_level as (
	select
		product_name,
		product_sku,
        product_variant,
        avg(product_price) / {{price_divisor}} as price,
        countif(action_type = 'purchase') as purchases,
        sum(product_revenue/{{price_divisor}}) as revenue,
        count(distinct if(action_type = 'view', client_id, null)) as num_users_viewed,
        countif(action_type = 'view') as total_views,
        (countif(action_type = 'purchase') / nullif(countif(action_type = 'view'), 0)) as conversion_rate
	from sessions
    {{ group_by_first(3) }}
    order by 2
)

select * from pivot_and_aggregate_sessions_to_product_level
