with

{{ get_product_sessions() }},

product_variants as (
	select * from {{ ref('product_variants') }}
),

products as (
	select
		time,
		product_name,
		price,
		sum(purchases) as total_purchases,
		sum(revenue) as total_revenue,
		if(sum(purchases) is not null, max(most_bought_variant), null) as most_bought_variant,
		sum(views) as total_views,
		sum(refunds) as total_refunds,
		sum(total_refunded_amount) as total_refunded_amount,
	from product_variants
    {{ group_by_first(3) }}
)

select * from products
