{%- set actions = ["view", "purchase"] -%}
{%- set channels = ["Social", "Referral", "Paid Search", "Organic Search", "Direct", "(Other)", "Email"] -%}
{%- set mediums = ["referral", "(none)", "organic", "product_sync", "email", "product_shelf", "cpc", "(not set)", "original"] -%}

with
{{ get_product_sessions() }},

products as select * from {{ ref('int_product_level') }},

traffic_source_performance as (
	select
		name,
		sku,
		variant,
		purchases,
		revenue,
		num_users_viewed,
		total_views,
		-- Channel
		countif(is_direct) as ga_direct_sessions,
		countif(channel = 'Direct') as direct_sessions,

		{% for action in actions %}
			{% for channel in channels %}
			countif(action = '{{action}}' and channel = '{{channel}}') as {{action}}s_from_{{channel}},
			{% endfor %}
		{% endfor %}

		-- Medium
		{% for action in actions %}
			{% for medium in mediums %}
			countif(action = '{{action}}' and medium = '{{medium}}') as {{action}}s_from_{{medium}},
			{% endfor %}
		{% endfor %}

	from products
	left outer join sessions using (sku)
)

select * from traffic_source_performance
