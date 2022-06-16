{% macro group_by_first(n) %}
group by {% for i in range(1, n + 1) %} {{ i }} {% if not loop.last %} , {% endif %} {% endfor %}
{% endmacro %}

{% macro not_null(col) %}
where {{col}} is not null
{% endmacro %}

{% macro is_set(col) %}
where {{col}} != "(not set)"
{% endmacro %}

{% macro trim_prod_name(parent) %}
if(
    {{parent}}.variant is not null,
    array_to_string(
	    array(
	    	select * except(offset)
	    	from {{parent}}.name_arr with offset
	    	where offset < array_length({{parent}}.name_arr) - 1
	    )
	    , ' '
    ),
    {{parent}}.full_product_name
)
{% endmacro %}

{% macro get_col_vals(col) %}
select
	{{col}},
from renamed
group by {{col}}
{% endmacro %}
