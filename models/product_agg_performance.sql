with

{{ get_product_sessions() }},

product_variants as (
	select * from {{ ref('product_variants') }}
),

product_performance as (
	select
		utc_hour,
		name,
		max(most_bought_variant),
		sum(revenue) as total_revenue,
	from product_variants
	group by name, utc_hour
)

select * from product_performance
