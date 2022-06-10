with cte as (
	select
		p.v2ProductName as name,
		h.transaction.transactionId as transaction_id,
		sum(p.productRevenue/1000000) as revenue,
		sum(p.productQuantity) as quantity
	from {{ source('umg_ga', 'ga_sessions_cut') }}, unnest(hits) as h,  unnest(h.product) as p
	-- where {{tableRange()}}
	{{ group_by(2) }}
)

select
	name as product_name,
	sum(revenue) as revenue,
	sum(quantity) as quantity,
	count(distinct transaction_id) as transactions
from cte
group by 1
order by revenue desc
