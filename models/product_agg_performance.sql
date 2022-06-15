with

{{ get_product_sessions() }},

product_variants as (
	select * from {{ ref('product_variants') }}
),

product_performance as (
	select
		time,
		product_name,
		max(most_bought_variant),
		sum(revenue) as total_revenue,
	from product_variants
	{{ group_by_first(2) }}
)

select * from product_performance
