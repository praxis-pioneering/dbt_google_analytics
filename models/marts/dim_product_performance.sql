SELECT 
  *, 
  sum(revenue/nullif(quantity,0)) as avgRev,
  sum(quantity/nullif(transactions,0)) as avgQuantity 
FROM {{ref('stg_ga__revenue')}}
GROUP BY 1,2,3,4
ORDER BY 2 DESC