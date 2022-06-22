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

group_by_date as (
	select
		date,
		sum(views) as views,
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
		concat(date) as inc_uk
	from {{ ref('products_daily') }}
	{% if is_incremental() %}
		where date >= (select max(date) from {{ this }})
	{% endif %}
    group by date
),

store as (
	select
		*,
		safe_divide(purchases, views) as conversion_rate,
	from group_by_date
)

select * from store
