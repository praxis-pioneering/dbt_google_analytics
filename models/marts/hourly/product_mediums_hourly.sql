{{
	config(
		materialized='incremental',
		unique_key='inc_uk'

	)
}}

{%- set actions = ["view", "purchase"] -%}
{%- set mediums = ["referral", "organic", "product_sync", "email", "product_shelf", "cpc", "original"] -%}

with

product_medium_stats as (
	select
		time,
		date,
		product_id,
		max(product_name) as product_name,
		{% for action in actions %}
		{% for medium in mediums %}
			sum({{medium}}_medium_{{action}}s) as {{medium}}_medium_{{action}}s,
			{% if action == "purchase" %}
				sum({{medium}}_medium_revenue) as {{medium}}_medium_revenue,
				sum({{medium}}_medium_purchases) / nullif(sum({{medium}}_medium_views),0) as {{medium}}_medium_conversion_rate,
			{% endif %}
		{% endfor %}
		{% endfor %}
		concat(time, product_id) as inc_uk
	from {{ ref('product_variant_mediums_hourly') }}
	{% if is_incremental() %}
		where time >= (select max(time) from {{ this }})
	{% endif %}
	{{ group_by_first(3) }}
)

select * from product_medium_stats
