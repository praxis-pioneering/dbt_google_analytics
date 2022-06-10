with

sessions as (
	select * from {{ ref('stg_ga__sessions') }}
),

pivot_and_aggregate_sessions_to_product_level as (
	select
		product_list_name,
		product_list_position,
		product_local_price,
		product_local_refund_amount,
		product_local_revenue,
		product_brand,
		product_price,
		product_quantity,
		product_refund_amount,
		product_revenue,
		product_sku,
		product_variant,
		product_category,
		product_name,
	from sessions
)
