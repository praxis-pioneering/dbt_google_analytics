with

{{ get_product_sessions() }},

products as (
	select * from {{ ref('int_common_product_name') }}
),

product_performance as (
	select
		name as name,
		last_value(variant)
		over (
			partition by name
			order by purchases
			-- rows between unbounded preceding and unbounded following
		) as most_bought_variant,
		sum(revenue) as total_revenue,
	from products
	{{ group_by_first(1) }}
)

select * from product_performance
