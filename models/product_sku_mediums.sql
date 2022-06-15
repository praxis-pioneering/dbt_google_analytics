{%- set actions = ["view", "purchase"] -%}
{%- set mediums = ["referral", "organic", "product_sync", "email", "product_shelf", "cpc", "original"] -%}

with
{{ get_product_sessions() }},

products as (
	select * from {{ ref('int_product_sku_level') }}
),

product_medium_counts as (
	select
		name,
		sku,
		variant,
		price,
		total_views,
		utc_hour,
		unix_hour,
		{% for action in actions %}
		{% for medium in mediums %}
		countif(action = '{{action}}' and medium = '{{medium}}') as {{medium}}_{{action}}s,
		{% endfor %}
		{% endfor %}
	from products
	left outer join sessions using (sku)
	{{ group_by_first(7) }}
),

product_medium_stats as (
	select
		*,
		{% for medium in mediums %}
		price * {{medium}}_purchases as {{medium}}_revenue,
		{{medium}}_purchases / nullif({{medium}}_views,0) as {{medium}}_conversion_rate,
		{% endfor %}
	from product_medium_counts
)

select * from product_medium_stats
