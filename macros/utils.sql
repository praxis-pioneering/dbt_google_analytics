{% macro group_by_first(n) %}
group by {% for i in range(1, n + 1) %} {{ i }} {% if not loop.last %} , {% endif %} {% endfor %}
{% endmacro %}

{% macro not_null(col) %}
where {{col}} is not null
{% endmacro %}

{% macro is_set(col) %}
where {{col}} != "(not set)"
{% endmacro %}