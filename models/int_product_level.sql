{{ config(materialized='ephemeral') }}
{%- set price_divisor = 1000000 -%} -- ga money values are x10^6

with

{{ get_product_sessions() }},

product_sku_level as (
	select
		max(product_name) as product_name,
		product_sku as sku,
        if (product_variant != '(not set)', product_variant, null) as variant,
		split(max(product_name), ' - ') as name_arr,
        avg(product_price) / {{price_divisor}} as price,
        countif(action_type = 'purchase') as purchases,
        sum(product_revenue/{{price_divisor}}) as revenue,
        count(distinct if(action_type = 'view', client_id, null)) as num_users_viewed,
        countif(action_type = 'view') as total_views,
        countif(action_type = 'refund') as refunds,
        sum(product_refund_amount/{{price_divisor}}) as total_refund_amount,
        
	from sessions
    group by sku, variant
    order by 2
),

products_with_common_name as (
	select
		{{ trim_prod_name('product_sku_level') }} as name,
		*,
        last_value(variant)
		over (
			partition by {{ trim_prod_name('product_sku_level') }}
			order by purchases
			rows between unbounded preceding and unbounded following
		) as most_bought_variant,
	from product_sku_level
)

select * from products_with_common_name
