{%- set actions = ["view", "purchase"] -%}
{%- set channels = ["social", "referral", "paid_search", "organic_search", "direct", "email"] -%}

with

store_channel_stats as (
	select
		date,
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
	from {{ ref('product_channels_daily') }}
	{% if is_incremental() %}
		where date >= (select max(date) from {{ this }})
	{% endif %}
	group by date
)

select * from store_channel_stats
