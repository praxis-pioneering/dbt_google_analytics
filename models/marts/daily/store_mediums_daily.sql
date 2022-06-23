{%- set actions = ["view", "purchase"] -%}
{%- set mediums = ["referral", "organic", "product_sync", "email", "product_shelf", "cpc", "original"] -%}

with

store_medium_stats as (
	select
		date,
		{% for action in actions %}
		{% for medium in mediums %}
			sum({{medium}}_medium_{{action}}s) as {{medium}}_medium_{{action}}s,
			{% if action == "purchase" %}
				sum({{medium}}_medium_revenue) as {{medium}}_medium_revenue,
				sum({{medium}}_medium_purchases) / nullif(sum({{medium}}_medium_views),0) as {{medium}}_medium_conversion_rate,
			{% endif %}
		{% endfor %}
		{% endfor %}
	from {{ ref('product_mediums_daily') }}
	{% if is_incremental() %}
		where date >= (select max(date) from {{ this }})
	{% endif %}
	group by date
)

select * from store_medium_stats
