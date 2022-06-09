with cte as 
 (SELECT 
  p.v2ProductName name
  , h.transaction.transactionId transaction_id
  , sum(p.productRevenue/1000000) revenue
  , sum(p.productQuantity) quantity
  FROM {{ source('umg_ga', 'ga_sessions_20220606') }}, unnest(hits) as h,  unnest(h.product) as p
  -- WHERE {{tableRange()}}
  {{ group_by(2) }})

SELECT name product_name
  , sum(revenue) revenue
  , sum(quantity) quantity
  , count(distinct transaction_id) transactions
  FROM cte
  group by 1
  order by revenue desc