{{
	config(
		materialized='incremental',
		unique_key='time'
	)
}}

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
	{% if is_incremental() %}
		where time >= (select max(time) from {{ this }})
	{% endif %}
)

select * from product_sku_performance
