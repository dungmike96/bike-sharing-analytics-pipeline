{% macro format_bi_amount(column_name) %}
    CASE 
        WHEN COALESCE({{ column_name }}, 0) = 0 THEN '0'
        WHEN {{ column_name }} >= 1000000000 THEN CONCAT(ROUND({{ column_name }} / 1000000000, 1), 'B')
        WHEN {{ column_name }} >= 1000000 THEN CONCAT(ROUND({{ column_name }} / 1000000, 1), 'M')
        WHEN {{ column_name }} >= 1000 THEN CONCAT(ROUND({{ column_name }} / 1000, 1), 'K')
        ELSE CAST({{ column_name }} AS STRING)
    END
{% endmacro %}