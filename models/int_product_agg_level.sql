with

{{ get_product_sessions() }},

products as (
	select * from {{ ref('int_product_sku_level') }}
),

product_performance as (
	select
		name,
		price,
		sum(purchases) as total_purchases,
		sum(revenue) as total_revenue,
		if(sum(purchases) is not max(most_bought_variant),
		sum(total_views) as total_views,
		sum(refunds) as total_refunds,
		sum(total_refunded_amount) as total_refunded_amount,
	from products
	{{ group_by_first(2) }}
)

select * from product_performance
