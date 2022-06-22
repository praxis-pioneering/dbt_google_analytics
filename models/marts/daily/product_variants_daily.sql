{{
	config(
		materialized='incremental',
		unique_key = 'inc_uk'
	)
}}

{%- set price_divisor = 1000000 -%} -- ga money values are x10^6

{%- set actions = ["view", "purchase"] -%}
{%- set channels = ["social", "referral", "paid_search", "organic_search", "direct", "email"] -%}
{%- set mediums = ["referral", "organic", "product_sync", "email", "product_shelf", "cpc", "original"] %}

with

product_variants_daily as (
    select
		date,
		product_id,
		sku,
		product_name,
		variant,
		price,
		sum(views) as views,
		sum(num_users_viewed) as num_users_viewed,
		sum(purchases) as purchases,
        sum(revenue) as revenue,
        sum(refunds) as refunds,
        sum(total_refunded_amount) as total_refunded_amount,
		sum(direct_sessions) as direct_sessions,
		{% for action in actions %}
		{% for channel in channels %}
		sum({{channel}}_channel_{{action}}s) as {{channel}}_channel_{{action}}s,
		{% endfor %}
		{% endfor %}
		{% for action in actions %}
		{% for medium in mediums %}
		sum({{medium}}_medium_{{action}}s) as {{medium}}_medium_{{action}}s,
		{% endfor %}
		{% endfor %}
        last_value(variant)
		over (
			partition by date, product_id
			order by sum(purchases)
			rows between unbounded preceding and unbounded following
		) as most_bought_variant,
		concat(date, sku, product_id) as inc_uk
    from {{ ref('product_variants_hourly') }}
	{% if is_incremental() %}
	where date >= (select max(date) from {{ this }})
	{% endif %}
    {{ group_by_first(6) }}
)

select * from product_variants_daily

