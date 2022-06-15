{{ config(materialized='ephemeral') }}

{%- set price_divisor = 1000000 -%} -- ga money values are x10^6

{%- set actions = ["view", "purchase"] -%}
{%- set channels = ["Social", "Referral", "Paid Search", "Organic Search", "Direct", "Email"] -%}
{%- set mediums = ["referral", "organic", "product_sync", "email", "product_shelf", "cpc", "original"] %}


with

sessions as (
	select * from {{ ref('stg_ga__sessions') }} as s
    where s.sku != '(not set)' and
    s.product_name != '(not set)'
	-- some where clause here for incremental?
),

group_by_time_pivot_to_products as (
    select
		utc_hour as time,
		sku,
		nullif(product_variant, '(not set)') as variant,
		max(product_name) as full_product_name, -- filters out weird alt names e.g. "the â€œshimmering beautifulâ€ wrap dress limited edition"
		split(max(product_name), ' - ') as name_arr,
        avg(product_price) / {{price_divisor}} as price,
		countif(action = 'view') as views,
		count(distinct if(action = 'view', client_id, null)) as num_users_viewed,
		countif(action = 'purchase') as purchases,
        sum(product_revenue/{{price_divisor}}) as revenue,
        countif(action = 'refund') as refunds,
        sum(product_refund_amount/{{price_divisor}}) as total_refunded_amount,
		array_agg(client_id) as client_ids,
		array_agg(action) as actions,
		countif(is_direct) as ga_direct_sessions,
		countif(channel = 'Direct') as true_direct_sessions,
		array_agg(channel) as channels,
		array_agg(medium) as mediums,
		{% for action in actions %}
		{% for channel in channels %}
		countif(action = '{{action}}' and channel = '{{channel}}') as {{channel | lower | replace(" ","_")}}_channel_{{action}}s,
		{% endfor %}
		{% endfor %}
		{% for action in actions %}
		{% for medium in mediums %}
		countif(action = '{{action}}' and medium = '{{medium}}') as {{medium}}_medium_{{action}}s,
		{% endfor %}
		{% endfor %}
    from sessions
    {{ group_by_first(3) }}
),

add_norm_product_name as (
	select
		{{ trim_prod_name('group_by_time_pivot_to_products') }} as product_name,
		*,
        last_value(variant)
		over (
			partition by time, {{ trim_prod_name('group_by_time_pivot_to_products') }}
			order by purchases
			rows between unbounded preceding and unbounded following
		) as most_bought_variant,
	from group_by_time_pivot_to_products
)

select * from add_norm_product_name
