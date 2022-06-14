{%- set actions = ["view", "purchase"] -%}
{%- set mediums = ["referral", "organic", "product_sync", "email", "product_shelf", "cpc", "original"] -%}

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
		price,
		{% for action in actions %}
		{% for medium in mediums %}
		countif(action = '{{action}}' and medium = '{{medium}}') as {{action}}s_from_{{medium}},
		{% if action == "purchase" %}
		price * countif(action = '{{action}}' and medium = '{{medium}}') as rev_from_{{medium}},
		{% endif %}
		{% endfor %}
		{% endfor %}
	from products
	left outer join sessions using (sku)
	{{ group_by_first(3) }}
)

select * from traffic_source_performance
