with cte as 
 (SELECT 
  p.v2ProductName name
  , h.transaction.transactionId transaction_id
  , sum(p.productRevenue/1000000) revenue
  , sum(p.productQuantity) quantity
  FROM {{ source('test', 'ga_sessions_*') }}, unnest(hits) as h,  unnest(h.product) as p
  -- WHERE {{tableRange()}}
  {{ group_by(2) }})

SELECT 
  name as product_name
  , sum(revenue) as revenue
  , sum(quantity) as quantity
  , count(distinct transaction_id) transactions
  FROM cte
  group by 1
  order by revenue desc