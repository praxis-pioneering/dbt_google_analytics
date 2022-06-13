{{ config(materialized='ephemeral') }}

with 
products as (
	select * from {{ ref('product_sku_performance') }}
),
products_with_split_name as (
	select
		*,
		split(name, ' - ') as name_arr
	from products
)

select * from products_with_split_name
