{{
	config(
		materialized='incremental',
		unique_key = 'inc_uk'
	)
}}

{%- set price_divisor = 1000000 -%} -- ga money values are x10^6

{%- set actions = ["view", "purchase"] -%}
{%- set channels = ["Social", "Referral", "Paid Search", "Organic Search", "Direct", "Email"] -%}
{%- set mediums = ["referral", "organic", "product_sync", "email", "product_shelf", "cpc", "original"] %}

with

group_by_time_pivot_to_products as (
    select
		utc_hour as time,
		date,
		product_id,
		sku,
		nullif(product_variant, '(not set)') as variant,
		price,
		max(full_product_name) as full_product_name, -- filters out weird alt names e.g. "the â€œshimmering beautifulâ€ wrap dress limited edition"
		split(max(full_product_name), ' - ') as name_arr,
		countif(action = 'view') as views,
		count(distinct if(action = 'view', client_id, null)) as num_users_viewed,
		countif(action = 'purchase') as purchases,
        sum(product_revenue/{{price_divisor}}) as revenue,
        countif(action = 'refund') as refunds,
        sum(product_refund_amount/{{price_divisor}}) as total_refunded_amount,
		array_agg(client_id) as client_ids,
		array_agg(action) as actions,
		countif(is_direct) as direct_sessions,
		array_agg(channel) as channels,
		array_agg(medium) as mediums,
		nullif(approx_top_count(ad_campaign, 1)[offset(0)].value, '(not set)') as best_ad_campaign, 
		{% for action in actions %}
		{% for channel in channels %}
		countif(action = '{{action}}' and channel = '{{channel}}') as {{channel | lower | replace(" ","_")}}_channel_{{action}}s,
		{% endfor %}
		{% endfor %}
		{% for action in actions %}
		{% for medium in mediums %}
		countif(action = '{{action}}' and medium = '{{medium}}') as {{medium}}_medium_{{action}}s,
		{% endfor %}
		{% endfor %}
		concat(utc_hour, sku, product_id) as inc_uk
    from {{ ref('stg_ga__sessions') }}
	left join {{ ref('stg_shopify__product_variant') }} using (sku)
    where
	{% if is_incremental() %}
		utc_hour >= (select max(time) from {{ this }}) and
	{% endif %}
	sku != '(not set)' and full_product_name != '(not set)'
    {{ group_by_first(6) }}
	order by time
),

product_variants as (
	select
		{{ trim_prod_name('group_by_time_pivot_to_products') }} as product_name,
		*,
        last_value(variant)
		over (
			partition by time, product_id
			order by purchases
			rows between unbounded preceding and unbounded following
		) as most_bought_variant,
		safe_divide(purchases, views) as conversion_rate,
	from group_by_time_pivot_to_products
	
)

select * from product_variants

