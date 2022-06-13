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
    where s.product_sku != "(not set)"
)
{% endmacro %}

{% macro get_product_sessions_between(start, end) %}
sessions_between as (
	select * from {{ ref('stg_ga__sessions') }} as s
    where s.product_sku != "(not set)" and
    (s.date between '{{start}}' and '{{end}}')
)

{% endmacro %}