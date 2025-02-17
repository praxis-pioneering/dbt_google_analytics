{%- set actions = ["view", "purchase"] -%}
{%- set mediums = ["referral", "organic", "product_sync", "email", "product_shelf", "cpc", "original"] -%}

with

product_medium_stats as (
	select
		date,
		product_id,
		product_name,
   		sku,
   		variant,
		{% for action in actions %}
		{% for medium in mediums %}
			{{medium}}_medium_{{action}}s,
			{% if action == "purchase" %}
				price * {{medium}}_medium_purchases as {{medium}}_medium_revenue,
				{{medium}}_medium_purchases / nullif({{medium}}_medium_views,0) as {{medium}}_medium_conversion_rate,
			{% endif %}
		{% endfor %}
		{% endfor %}
	from {{ ref('product_variants_daily') }}
	{% if is_incremental() %}
		where date >= (select max(date) from {{ this }})
	{% endif %}
)

select * from product_medium_stats
