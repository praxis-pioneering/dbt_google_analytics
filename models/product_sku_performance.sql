{%- set price_divisor = 1000000 -%} -- ga money values are x10^6

with

product_sku_performance as (
	select
		name,
		variant,
		sku,
		price,
		purchases,
		revenue,
		num_users_viewed,
		total_views,
        (purchases / nullif(total_views, 0)) as conversion_rate,
		refunds,
		total_refund_amount,
	from {{ ref('int_product_level') }}
    {{ group_by_first(3) }}
    order by sku
)

select * from product_sku_performance
