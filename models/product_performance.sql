with

{{ get_product_sessions() }},

products as select * from {{ ref('int_product_name_arr') }}

product_performance as (
	select
        array_to_string(
			array(
				select * except(offset)
				from int_data.arr with offset
				where offset < array_length(int_data.arr) - 1
			)
			, ' '
        ) as name,
		/* variant, */
	from products
	/* {{ group_by_first(1) }} */
)

select * from product_performance
