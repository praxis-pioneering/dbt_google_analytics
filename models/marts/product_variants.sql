{%- set price_divisor = 1000000 -%} -- ga money values are x10^6

with

sessions as (
	select * from {{ ref('int_sessions_grouped_by_time') }}
),

product_sku_level as (
	select
		time,
		max(product_name) as product_name, -- filters out weird alt names e.g. "the â€œshimmering beautifulâ€ wrap dress limited edition"
		sku,
		nullif(product_variant, '(not set)') as variant,
		split(max(product_name), ' - ') as name_arr,
        avg(product_price) / {{price_divisor}} as price,
		countif(action = 'view') as total_views,
		count(distinct if(action = 'view', client_id, null)) as num_users_viewed,
        countif(action = 'purchase') as purchases,
        sum(product_revenue/{{price_divisor}}) as revenue,
        countif(action = 'refund') as refunds,
        sum(product_refund_amount/{{price_divisor}}) as total_refunded_amount,
	from sessions
    group by time, sku, variant
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
