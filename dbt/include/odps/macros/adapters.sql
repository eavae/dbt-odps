{% macro lifecycle_clause(temporary) %}
  {{ return(adapter.dispatch('lifecycle_clause', 'dbt')(temporary)) }}
{%- endmacro -%}


{% macro odps__lifecycle_clause(temporary) %}
  {%- set lifecycle = config.get('lifecycle') -%}
  {%- if lifecycle is not none -%}
    lifecycle {{ lifecycle }}
  {%- elif temporary -%}
    lifecycle 1
  {%- endif %}
{%- endmacro -%}


{% macro table_properties_clause() %}
  {{ return(adapter.dispatch('table_properties_clause', 'dbt')()) }}
{%- endmacro -%}


{% macro odps__table_properties_clause() %}
  {%- set properties = config.get('properties', none) -%}
  {%- if properties is not none -%}
    tblproperties(
    {%- for k, v in properties.items() -%}'{{ k }}'='{{ v }}'
    {%- if not loop.last %},{% endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}


{% macro partitioned_by_clause() %}
  {{ return(adapter.dispatch('partitioned_by_clause', 'dbt')()) }}
{%- endmacro -%}


{% macro odps__partitioned_by_clause() %}
  {%- set partitioned_by = config.get('partitioned_by', none) -%}
  {%- if partitioned_by is not none -%}
    partitioned by (
    {%- for item in partitioned_by -%}
      {{ item['col_name'] }} {{ item['data_type'] or 'string' }}{%- if item['comment'] %} comment '{{ item['comment'] }}'{%- endif -%}
      {%- if not loop.last %}, {% endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}


{% macro create_table_like(relation, from_relation) %}
  {{ return(adapter.dispatch('create_table_like', 'dbt')(relation, from_relation)) }}
{%- endmacro -%}


{%- macro odps__current_timestamp() -%}
  current_timestamp()
{%- endmacro -%}


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
