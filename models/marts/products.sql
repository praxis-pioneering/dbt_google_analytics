with

{{ get_product_sessions() }},

products as (
	select * from {{ ref('int_product_sku_level') }}
),

product_performance as (
	select
		utc_hour,
		name,
		price,
		sum(purchases) as total_purchases,
		sum(revenue) as total_revenue,
		if(sum(purchases) is not null, max(most_bought_variant), null) as most_bought_variant,
		sum(total_views) as total_views,
		sum(refunds) as total_refunds,
		sum(total_refunded_amount) as total_refunded_amount,
	from products
    group by name, price, utc_hour
)

select * from product_performance
