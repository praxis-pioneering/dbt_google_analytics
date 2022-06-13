{% macro group_by_first(n) %}
group by {% for i in range(1, n + 1) %} {{ i }} {% if not loop.last %} , {% endif %} {% endfor %}
{% endmacro %}

{% macro not_null(col) %}
where {{col}} is not null
{% endmacro %}

{% macro is_set(col) %}
where {{col}} != "(not set)"
{% endmacro %}

{% macro get_product_sessions() %}
sessions as (
	select * from {{ ref('stg_ga__sessions') }} as s
    where s.product_sku != '(not set)' and
    s.product_name != '(not set)'
)
{% endmacro %}

{% macro get_product_sessions_between(start, end) %}
sessions_between as (
	select * from {{ ref('stg_ga__sessions') }} as s
    where s.product_sku != '(not set)' and
    (s.date between '{{start}}' and '{{end}}')
)
{% endmacro %}

{% macro trim_prod_name() %}
if(
    strpos(products.name, '-') != 0,
    array_to_string(
	    array(
	    	select * except(offset)
	    	from products.name_arr with offset
	    	where offset < array_length(products.name_arr) - 1
	    )
	    , ' '
    ),
    products.name
)
{% endmacro %}
