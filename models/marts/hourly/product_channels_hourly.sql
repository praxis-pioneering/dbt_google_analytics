{{
	config(
		materialized='incremental',
		unique_key='inc_uk'
	)
}}

{%- set actions = ["view", "purchase"] -%}
{%- set channels = ["social", "referral", "paid_search", "organic_search", "direct", "email"] -%}

with

product_channel_stats as (
	select
		time,
		date,
		product_id,
		product_name,
		sum(direct_sessions) as direct_sessions,
		{% for action in actions %}
		{% for channel in channels %}
			sum({{channel}}_channel_{{action}}s) as {{channel}}_channel_{{action}}s,
			{% if action == "purchase" %}
				sum({{channel}}_channel_revenue) as {{channel}}_channel_revenue,
				sum({{channel}}_channel_purchases) / nullif(sum({{channel}}_channel_views),0) as {{channel}}_channel_conversion_rate,
			{% endif %}
		{% endfor %}
		{% endfor %}
		concat(time, product_id) as inc_uk
	from {{ ref('product_variant_channels_hourly') }}
	{% if is_incremental() %}
		where time >= (select max(time) from {{ this }})
	{% endif %}
	{{ group_by_first(4) }}
)

select * from product_channel_stats
