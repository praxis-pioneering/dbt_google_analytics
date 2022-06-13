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
),
products_with_common_name as (
	select
		{{ trim_prod_name() }} as name,
		*,
)

select * from products_with_common_name
