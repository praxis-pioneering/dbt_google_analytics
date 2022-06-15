{%- set actions = ["view", "purchase"] -%}
{%- set mediums = ["referral", "organic", "product_sync", "email", "product_shelf", "cpc", "original"] -%}

with

sessions as (
	select * from {{ ref('int_sessions_grouped_by_time') }}
),

products as (
	select * from {{ ref('product_variants') }}
),

product_medium_counts as (
	select
		time,
		name,
		sku,
		variant,
		price,
		total_views,
		{% for action in actions %}
		{% for medium in mediums %}
		countif(action = '{{action}}' and medium = '{{medium}}') as {{medium}}_{{action}}s,
		{% endfor %}
		{% endfor %}
	from products
	left outer join sessions using (sku)
	{{ group_by_first(6) }}
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
