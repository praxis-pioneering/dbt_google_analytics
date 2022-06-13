{%- set price_divisor = 1000000 -%} -- ga money values are x10^6

with

{{ get_product_sessions() }},

product_sku_level as (
	select
		product_name,
		product_sku as sku,
        if (product_variant != '(not set)', product_variant, null) as variant,
        avg(product_price) / {{price_divisor}} as price,
        countif(action_type = 'purchase') as purchases,
        sum(product_revenue/{{price_divisor}}) as revenue,
        count(distinct if(action_type = 'view', client_id, null)) as num_users_viewed,
        countif(action_type = 'view') as total_views,
        (countif(action_type = 'purchase') / nullif(countif(action_type = 'view'), 0)) as conversion_rate,
        countif(action_type = 'refund') as refunds,
        sum(product_refund_amount/{{price_divisor}}) as total_refund_amount,
	from sessions
    {{ group_by_first(3) }}
    order by 2
)

select * from product_sku_level
