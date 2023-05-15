
{% macro odps__create_view_as(relation, sql) -%}
  create or replace view {{ relation }}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(sql) }}
    {%- endif %}
  as {{ sql }};
{%- endmacro %}

{% macro odps__create_table_as(temporary, relation, sql) -%}
  {%- if temporary -%}
    {% call statement('drop_before_create') -%}
      {%- if not relation.type -%}
        {% do exceptions.raise_database_error("Cannot drop a relation with a blank type: " ~ relation.identifier) %}
      {%- elif relation.type in ('table') -%}
          drop table if exists {{ relation }}
      {%- elif relation.type == 'view' -%}
          drop view if exists {{ relation }}
      {%- else -%}
        {% do exceptions.raise_database_error("Unknown type '" ~ relation.type ~ "' for relation: " ~ relation.identifier) %}
      {%- endif -%}
    {%- endcall -%}
  {%- endif -%}

  create table if not exists {{ relation }}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(sql) }}
      {{ get_columns_spec_ddl() }}
      {%- set sql = get_select_subquery(sql) %}
    {%- endif %}
    {{ lifecycle_clause(temporary) }}
  as {{ sql }}
{%- endmacro %}

{% macro odps__create_table_like(relation, from_relation) %}
  {%- if from_relation.type != 'table' -%}
    {%- do exceptions.raise_database_error("Cannot create a table like a non-table relation: " ~ from_relation.identifier) -%}
  {%- endif -%}

  {%- set partitioned_by = config.get('partitioned_by', []) -%}
  {%- set exclude_col_names = partitioned_by | map(attribute='col_name') | list -%}
  {%- set columns = adapter.get_columns_in_relation(from_relation) -%}

  create table if not exists {{ relation }} (
  {% for column in columns -%}
    {%- if column.name not in exclude_col_names -%}
    {{ column.quoted }} {{ column.data_type }} comment '{{ column.comment or "" }}'{{ ',' if not loop.last }}
    {%- endif -%}
  {% endfor %}
  )
  {{ partitioned_by_clause() }}
  {{ clustered_by_clause() }}
  {{ table_properties_clause() }}
  {{ lifecycle_clause(false) }}
{%- endmacro -%}
