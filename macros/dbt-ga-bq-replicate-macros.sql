{% macro uid() -%}

 concat(fullVisitorId, cast(visitId as string)) as uid

{%- endmacro %}

{% macro tableRange() -%}

_table_suffix between '{{var("rangeStart")}}' and '{{var("rangeEnd")}}'

{%- endmacro %}

{% macro parseDate() -%}

parse_date("%Y%m%d", date) date_

{%- endmacro %}

{% macro eventCase(value) -%}

sum(CASE when h.eventInfo.eventAction = '{{value}}' then 1 else 0 end) as {{value | replace('-', '_')}}

{%- endmacro %}

{% macro unnest(field, alias) -%}

unnest({{field}}) as {{alias}}

{%- endmacro %}

{% macro group_by(n) %}

  GROUP BY {% for i in range(1, n + 1) %} {{ i }} {% if not loop.last %} , {% endif %} {% endfor %}

{% endmacro %}

{% macro getBounces(field, alias) -%}

SELECT
  {{ alias }} 
  , sum(CASE WHEN (noInteractions = 1 and isInteraction = true) or noInteractions = 0 THEN bounces ELSE null END) AS bounces
  FROM (SELECT
        {{ field }} {{ alias }}
        , COUNTIF(h.isInteraction = true) OVER (PARTITION BY fullVisitorId, visitId) AS noInteractions
        , h.isInteraction 
        , totals.bounces
        FROM {{var('tableName')}}, {{ unnest('hits', 'h') }}
	WHERE {{tableRange()}}
        GROUP BY 1, fullVisitorId, h.isInteraction, totals.bounces, visitId)
  {{ group_by(1) }}
  ORDER BY 2 DESC

{%- endmacro -%}

{% macro getSessions(field, alias) -%}

	SELECT 
  {{ alias }}
  , sum(CASE WHEN hitNumber = first_hit THEN visits ELSE null END) AS sessions
  FROM (SELECT
        {{ field }} {{ alias }}
        , MIN(h.hitNumber) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_hit
        , h.hitNumber
        , totals.visits
        FROM {{var('tableName')}}, {{ unnest('hits', 'h') }}
	WHERE {{tableRange()}}
        GROUP BY 1, h.hitNumber, fullVisitorId, visitStartTime, totals.visits, visitId
        ORDER BY 2 DESC)
      {{ group_by(1) }}
      ORDER BY 2 DESC

{%- endmacro %}
