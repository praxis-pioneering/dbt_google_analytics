with

{{ get_product_sessions() }},

products as (
	select * from {{ ref('int_product_sku_level') }}
),

product_performance as (
	select
		utc_hour,
		name,
		max(most_bought_variant),
		sum(revenue) as total_revenue,
	from products
	group by name, utc_hour
)

select * from product_performance
