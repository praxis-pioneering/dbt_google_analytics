{{ config(materialized='ephemeral') }}

with

{{ get_product_sessions() }},

sessions_grouped_by_time as (
    select
		utc_hour as time,
		product_name,
		sku,
		product_variant,
		product_price,
		product_revenue,
		product_refund_amount,
		array_agg(client_id) as client_ids,
		array_agg(action) as actions,
		countif(action = 'view') as views,
		count(distinct if(action = 'view', client_id, null)) as num_users_viewed,
		countif(action = 'purchase') as purchases,
		countif(action = 'refund') as refunds,
		countif(is_direct) as ga_direct_sessions,
		array_agg(channel) as channels,
		array_agg(medium) as mediums
    from sessions
    {{ group_by_first(7) }}
)

select * from sessions_grouped_by_time
where sku = '838532832'
