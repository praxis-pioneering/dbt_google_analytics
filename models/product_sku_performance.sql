with

product_sku_performance as (
	select
		utc_hour,
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
		total_refunded_amount,
	from {{ ref('product_variants') }}
    order by sku
)

select * from product_sku_performance
