{%- set actions = ["view", "purchase"] -%}
{%- set channels = ["Social", "Referral", "Paid Search", "Organic Search", "Direct", "Email"] -%}

with
{{ get_product_sessions() }},

products as (
	select * from {{ ref('int_product_sku_level') }}
),

product_channel_counts as (
	select
		name,
		sku,
		variant,
		price,
		total_views,
		countif(is_direct) as ga_direct_sessions,
		countif(channel = 'Direct') as direct_sessions,
		{% for action in actions %}
		{% for channel in channels %}
		countif(action = '{{action}}' and channel = '{{channel}}') as {{channel | lower | replace(" ","_")}}_{{action}}s,
		{% endfor %}
		{% endfor %}
	from products
	left outer join sessions using (sku)
	{{ group_by_first(5) }}
),

product_channel_stats as (
	select
		*,
		{% for channel in channels | map("lower") | map("replace", " ", "_") %}
		price * {{channel}}_purchases as {{channel}}_revenue,
		{{channel}}_purchases / nullif({{channel}}_views,0) as {{channel}}_conversion_rate,
		{% endfor %}
	from product_channel_counts
)

select * from product_channel_stats
