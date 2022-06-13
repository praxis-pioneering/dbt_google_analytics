{{ config(materialized='ephemeral') }}

with products as {{ ref('product_sku_performance') }}

select
	*,
	split(name, ' - ') as name_arr
from products
