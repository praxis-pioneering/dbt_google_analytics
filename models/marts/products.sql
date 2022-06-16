{{
	config(
		materialized='incremental',
		unique_key='time'
	)
}}

with

products as (
	select
		time,
		product_name,
		sum(views) as total_views,
		sum(purchases) as total_purchases,
		sum(revenue) as total_revenue,
		if(sum(purchases) is not null, max(most_bought_variant), null) as most_bought_variant,
		sum(refunds) as total_refunds,
		sum(total_refunded_amount) as total_refunded_amount,
	from {{ ref('product_variants') }}
	{% if is_incremental() %}
		where time >= (select max(time) from {{ this }})
	{% endif %}
    {{ group_by_first(2) }}
)

select * from products
