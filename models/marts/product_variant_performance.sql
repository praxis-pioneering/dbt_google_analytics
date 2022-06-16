with

product_sku_performance as (
	select
		time,
		product_name,
		variant,
		sku,
		purchases,
		revenue,
		num_users_viewed,
		views,
        safe_divide(purchases, views) as conversion_rate,
		refunds,
		total_refunded_amount,
	from {{ ref('product_variants') }}
)

select * from product_sku_performance
