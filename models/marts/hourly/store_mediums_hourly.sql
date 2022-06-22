{{
	config(
		materialized='incremental',
		unique_key='inc_uk'
	)
}}

{%- set actions = ["view", "purchase"] -%}
{%- set mediums = ["referral", "organic", "product_sync", "email", "product_shelf", "cpc", "original"] -%}

with

store_medium_stats as (
	select
		time,
		date,
		{% for action in actions %}
		{% for medium in mediums %}
			{{medium}}_medium_{{action}}s,
			{% if action == "purchase" %}
				price * {{medium}}_medium_purchases as {{medium}}_medium_revenue,
				{{medium}}_medium_purchases / nullif({{medium}}_medium_views,0) as {{medium}}_medium_conversion_rate,
			{% endif %}
		{% endfor %}
		{% endfor %}
		inc_uk
	from {{ ref('store_hourly') }}
	{% if is_incremental() %}
		where time >= (select max(time) from {{ this }})
	{% endif %}
)

select * from store_medium_stats
