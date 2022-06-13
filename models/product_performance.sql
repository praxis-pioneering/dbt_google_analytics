with

{{ get_product_sessions() }},

products as (
	select * from {{ ref('int_product_name_arr') }}
),

product_performance as (
	select
        {{ trim_prod_name() }},
		sum(revenue) as total_revenue,
		-- variant
	from products
	{{ group_by_first(1) }}
)

select * from product_performance
