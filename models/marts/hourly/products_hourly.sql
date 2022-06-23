{{
	config(
		materialized='incremental',
		unique_key='inc_uk'
	)
}}

{%- set actions = ["view", "purchase"] -%}
{%- set channels = ["social", "referral", "paid_search", "organic_search", "direct", "email"] -%}
{%- set mediums = ["referral", "organic", "product_sync", "email", "product_shelf", "cpc", "original"] -%}

with

group_by_product as (
	select
		time,
		date,
		product_id,
		max(product_name) as product_name,
		sum(views) as views,
		sum(purchases) as purchases,
		sum(revenue) as revenue,
		if(sum(purchases) is not null, max(most_bought_variant), null) as most_bought_variant,
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
		concat(time, product_id) as inc_uk
	from {{ ref('product_variants_hourly') }}
	{% if is_incremental() %}
		where time >= (select max(time) from {{ this }})
	{% endif %}
    {{ group_by_first(3) }}
),

products as (
	select
		*,
		safe_divide(purchases, views) as conversion_rate,
	from group_by_product
)

select * from products
