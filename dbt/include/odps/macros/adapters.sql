{# tested #}
{% macro odps__list_relations_without_caching(relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    show tables
  {%- endcall %}

  {% set result = load_result('list_relations_without_caching') %}
  {% do return(result.table[0][0]) %}
{% endmacro %}

{% macro odps__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{# tested: using cast to convert types #}
{% macro odps__load_csv_rows(model, agate_table) %}
  {% set batch_size = get_batch_size() %}
  {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
  {% set bindings = [] %}
  {% set statements = [] %}

  {# get odps types #}
  {% set column_override = model['config'].get('column_types', {}) %}
  {% set data_types = {} %}
  {% for col_name in agate_table.column_names %}
    {% set inferred_type = adapter.convert_type(agate_table, loop.index0) %}
    {% set data_type = column_override.get(col_name, inferred_type) %}
    {% do data_types.update({col_name: data_type}) %}
  {% endfor %}

  {% for chunk in agate_table.rows | batch(batch_size) %}
      {% set sql %}
          insert into {{ this.render() }} ({{ cols_sql }}) values
          {% for row in chunk -%}
              ({%- for column in agate_table.column_names -%}
                  {%- if data_types[column] == 'string' -%}
                    '{{ row[column] }}'
                  {%- else -%}
                    cast('{{ row[column] }}' as {{ data_types[column] }})
                  {%- endif -%}
                  {%- if not loop.last%},{%- endif %}
              {%- endfor -%})
              {%- if not loop.last%},{%- endif %}
          {%- endfor %}
      {% endset %}

      {% do adapter.add_query(sql, abridge_sql_log=True) %}

      {% if loop.index0 == 0 %}
          {% do statements.append(sql) %}
      {% endif %}
  {% endfor %}

  {{ return(statements[0]) }}
{% endmacro %}

{# tested #}
{% macro odps__create_view_as(relation, sql) -%}
  create or replace view {{ relation }}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(sql) }}
    {%- endif %}
  as {{ sql }};
{%- endmacro %}

{# tested #}
{% macro odps__create_table_as(temporary, relation, sql) -%}
  create table {{ relation }}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(sql) }}
      {{ get_columns_spec_ddl() }}
      {%- set sql = get_select_subquery(sql) %}
    {%- endif %}
  as {{ sql }};
{%- endmacro %}

{# tested #}
{% macro default__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    alter {{ from_relation.type }} {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}

{# tested #}
{% macro show_create_table(relation) %}
  {% call statement('show_create_table', fetch_result=True) -%}
    show create table {{ relation }}
  {%- endcall %}

  {% set result = load_result('show_create_table') %}
  {% do return(result.table[0][0]) %}
{% endmacro %}
