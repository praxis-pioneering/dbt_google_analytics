{% macro group_by_first(n) %}

  GROUP BY {% for i in range(1, n + 1) %} {{ i }} {% if not loop.last %} , {% endif %} {% endfor %}

{% endmacro %}
