with

{{ get_product_sessions() }},

products as (
	select * from {{ ref('int_product_sku_level') }}
),

product_performance as (
	select
		name as name,
		max(most_bought_variant),
		sum(revenue) as total_revenue,
	from products
	{{ group_by_first(1) }}
)

select * from product_performance
