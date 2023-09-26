{# https://github.com/dbt-labs/dbt-core/blob/main/plugins/postgres/dbt/include/postgres/macros/adapters.sql #}

{% macro postgres__create_table_as(temporary, relation, sql) -%}
  {%- set unlogged = config.get('unlogged', default=false) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary -%}
    temporary
  {%- elif unlogged -%}
    unlogged
  {%- endif %} table {{ relation }}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{ get_assert_columns_equivalent(sql) }}
    {{ get_table_columns_and_constraints() }} 
{# added by ernesto #}
    {% if config.get('postgres_partition') %}
    PARTITION BY RANGE ( {{config.get('postgres_partition') }} );
    {%- endif %}
    {{create_monthly_partitions(this)}}
{# finish added by ernesto #}
    insert into {{ relation }} (
      {{ adapter.dispatch('get_column_names', 'dbt')() }}
    )
    {%- set sql = get_select_subquery(sql) %}
  {% else %}
    as
  {% endif %}
  (
    {{ sql }}
  );
{%- endmacro %}