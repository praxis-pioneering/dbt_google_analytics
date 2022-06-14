{%- set actions = ["view", "purchase"] -%}
{%- set channels = ["Social", "Referral", "Paid Search", "Organic Search", "Direct", "Email"] -%}

with
{{ get_product_sessions() }},

products as (
	select * from {{ ref('int_product_level') }}
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
		countif(action = '{{action}}' and channel = '{{channel}}') as {{action}}s_from_{{channel | lower | replace(" ","_")}},
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
		price * purchases_from_{{channel}} as rev_from_{{channel}},
		purchases_from_{{channel}} / nullif(views_from_{{channel}},0) as {{channel}}_conversion_rate,
		{% endfor %}
	from product_channel_counts
)

select * from product_channel_stats
