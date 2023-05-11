{% macro odps__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}

  {% call statement('rename_relation') -%}
    {% if not from_relation.type %}
      {% do exceptions.raise_database_error("Cannot rename a relation with a blank type: " ~ from_relation.identifier) %}
    {% elif from_relation.type in ('table') %}
        alter table {{ from_relation }} rename to {{ target_name }}
    {% elif from_relation.type == 'view' %}
        alter view {{ from_relation }} rename to {{ target_name }}
    {% else %}
      {% do exceptions.raise_database_error("Unknown type '" ~ from_relation.type ~ "' for relation: " ~ from_relation.identifier) %}
    {% endif %}
  {%- endcall %}
{% endmacro %}