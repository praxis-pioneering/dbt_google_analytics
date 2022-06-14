{%- set actions = ["view", "purchase"] -%}
{%- set channels = ["Social", "Referral", "Paid Search", "Organic Search", "Direct", "Email"] -%}

with
{{ get_product_sessions() }},

products as (
	select * from {{ ref('int_product_level') }}
),

traffic_source_performance as (
	select
		name,
		sku,
		variant,

		-- Channel
		countif(is_direct) as ga_direct_sessions,
		countif(channel = 'Direct') as direct_sessions,

		{% for action in actions %}
		{% for channel in channels %}
		countif(action = '{{action}}' and channel = '{{channel}}') as {{action}}s_from_{{channel | lower | replace(" ","_")}}_channel,
		{% endfor %}
		{% endfor %}

	from products
	left outer join sessions using (sku)
	{{ group_by_first(3) }}
)

select * from traffic_source_performance
