{{
	config(
		materialized='incremental',
		unique_key='time'
	)
}}

{%- set actions = ["view", "purchase"] -%}
{%- set channels = ["social", "referral", "paid_search", "organic_search", "direct", "email"] -%}
{%- set mediums = ["referral", "organic", "product_sync", "email", "product_shelf", "cpc", "original"] -%}

with

products as (
	select
		time,
		product_name,
		sum(views) as views,
		sum(purchases) as purchases,
		sum(revenue) as revenue,
		if(sum(purchases) is not null, max(most_bought_variant), null) as most_bought_variant,
		sum(refunds) as refunds,
		sum(refunded_amount) as total_refunded_amount,
		sum(direct_sessions) as direct_sessions,
		{% for action in actions %}
		{% for channel in channels %}
		sum({{channel}}_channel_{{action}}s),
		{% endfor %}
		{% endfor %}
		{% for action in actions %}
		{% for medium in mediums %}
		sum({{medium}}_medium_{{action}}s),
		{% endfor %}
		{% endfor %}

	from {{ ref('product_variants') }}
	{% if is_incremental() %}
		where time >= (select max(time) from {{ this }})
	{% endif %}
    {{ group_by_first(2) }}
)

select * from products
