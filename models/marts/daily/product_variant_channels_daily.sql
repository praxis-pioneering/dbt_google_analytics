{%- set actions = ["view", "purchase"] -%}
{%- set channels = ["social", "referral", "paid_search", "organic_search", "direct", "email"] -%}

with

product_channel_stats as (
	select
		date,
		product_id,
		product_name,
   		sku,
   		variant,
		direct_sessions,
		{% for action in actions %}
		{% for channel in channels %}
			{{channel}}_channel_{{action}}s,
			{% if action == "purchase" %}
				price * {{channel}}_channel_purchases as {{channel}}_channel_revenue,
				{{channel}}_channel_purchases / nullif({{channel}}_channel_views,0) as {{channel}}_channel_conversion_rate,
			{% endif %}
		{% endfor %}
		{% endfor %}
	from {{ ref('product_variants_daily') }}
	{% if is_incremental() %}
		where date >= (select max(date) from {{ this }})
	{% endif %}
)

select * from product_channel_stats
