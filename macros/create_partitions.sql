{% macro create_monthly_partitions(relation) %}

{% set partitions_query %}
select TO_CHAR(date_day, 'YYYYMMDD')::int as partition_field from 
(
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2023-09-01' as date)",
    end_date="cast('2023-10-01' as date)"
   )
}}
) spine
{% endset %}

{% set results = run_query(partitions_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{{print(results_list)}}
{% else %}
{% set results_list = [] %}
{% endif %}

{% for day in results_list %}
CREATE TABLE {{relation|replace('"', '')}}_{{day}} PARTITION OF {{relation|replace('"', '')}}__dbt_tmp
    FOR VALUES FROM ({{day - 1}}) TO ({{day}});
{% endfor %}

    
{% endmacro %}