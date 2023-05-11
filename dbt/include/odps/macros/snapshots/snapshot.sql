{% macro odps__snapshot_hash_arguments(args) -%}
    md5({%- for arg in args -%}
        nvl(cast({{ arg }} as string ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})
{%- endmacro %}

{% macro odps__snapshot_get_time() -%}
    {{ current_timestamp() }}
{%- endmacro %}

{% macro build_snapshot_full_refresh_insert_into(strategy, sql, target_relation) %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) | list -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {%- set source_columns = dest_columns
                            | rejectattr('name', 'equalto', 'dbt_scd_id')
                            | rejectattr('name', 'equalto', 'dbt_updated_at')
                            | rejectattr('name', 'equalto', 'dbt_valid_from')
                            | rejectattr('name', 'equalto', 'dbt_valid_to')
                            | list -%}
    {%- set source_cols_csv = source_columns | map(attribute='quoted') | join(', ') -%}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    select
        {{ source_cols_csv }},
        {# the flowing order is important here #}
        cast({{ strategy.updated_at }} as timestamp) as dbt_updated_at,
        cast({{ strategy.updated_at }} as timestamp) as dbt_valid_from,
        cast(nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as timestamp)  as dbt_valid_to,
        {{ strategy.scd_id }} as dbt_scd_id
    from ({{ sql }})
{% endmacro %}


{% macro odps__snapshot_merge_sql(target, source, insert_cols) -%}
    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id
    when matched
        and DBT_INTERNAL_DEST.dbt_valid_to is null
        and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
            then update
            set DBT_INTERNAL_DEST.dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    when not matched
        and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
            then insert values ({% for col in insert_cols %}DBT_INTERNAL_SOURCE.{{ col }}{% if not loop.last %}, {% endif %}{% endfor %})
{% endmacro %}


{% macro odps__snapshot_staging_table(strategy, source_sql, target_relation) -%}
    with snapshot_query as (
        {{ source_sql }}
    ),
    snapshotted_data as (
        select *,
            {{ strategy.unique_key }} as dbt_unique_key
        from {{ target_relation }}
        where dbt_valid_to is null
    ),
    insertions_source_data as (
        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            cast({{ strategy.updated_at }} as timestamp) as dbt_updated_at,
            cast({{ strategy.updated_at }} as timestamp) as dbt_valid_from,
            cast(nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as timestamp) as dbt_valid_to,
            {{ strategy.scd_id }} as dbt_scd_id
        from snapshot_query
    ),
    updates_source_data as (
        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            cast({{ strategy.updated_at }} as timestamp) as dbt_updated_at,
            cast({{ strategy.updated_at }} as timestamp) as dbt_valid_from,
            cast({{ strategy.updated_at }} as timestamp) as dbt_valid_to
        from snapshot_query
    ),
    {%- if strategy.invalidate_hard_deletes %}
    deletes_source_data as (
        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key
        from snapshot_query
    ),
    {% endif %}
    insertions as (
        select
            'insert' as dbt_change_type,
            source_data.*
        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and (
                {{ strategy.row_changed }}
            )
        )
    ),
    updates as (
        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.dbt_scd_id
        from updates_source_data as source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where (
            {{ strategy.row_changed }}
        )
    )
    {%- if strategy.invalidate_hard_deletes -%}
    ,
    deletes as (
        select
            'delete' as dbt_change_type,
            source_data.*,
            cast({{ snapshot_get_time() }} as timestamp) as dbt_valid_from,
            cast({{ snapshot_get_time() }} as timestamp) as dbt_updated_at,
            cast({{ snapshot_get_time() }} as timestamp) as dbt_valid_to,
            snapshotted_data.dbt_scd_id
        from snapshotted_data
        left join deletes_source_data as source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where source_data.dbt_unique_key is null
    )
    {%- endif %}
    select * from insertions
    union all
    select * from updates
    {%- if strategy.invalidate_hard_deletes %}
    union all
    select * from deletes
    {%- endif %}
{%- endmacro %}

{% macro odps_build_snapshot_staging_table(strategy, sql, target_relation, target_relation_exists) %}
    {% set temp_identifier = target_relation.identifier ~ '__dbt_tmp' %}
    {% set temp_relation = api.Relation.create(identifier=temp_identifier,
                                               schema=target_relation.schema,
                                               database=target_relation.database,
                                               type='table') %}

    {% if target_relation_exists %}
        {% set select_sql = snapshot_staging_table(strategy, sql, target_relation) %}
    {% else %}
        {% set select_sql %}
        select
            ODPS_SNAPSHOT_FULL_REFRESH.*,
            cast({{ strategy.updated_at }} as timestamp) as dbt_updated_at,
            cast({{ strategy.updated_at }} as timestamp) as dbt_valid_from,
            cast(nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as timestamp)  as dbt_valid_to,
            {{ strategy.scd_id }} as dbt_scd_id
        from ({{ sql }}) as ODPS_SNAPSHOT_FULL_REFRESH
        {% endset %}
    {% endif %}

    {% call statement('build_snapshot_staging_relation') %}
        {{ create_table_as(True, temp_relation, select_sql) }}
    {% endcall %}

    {% do return(temp_relation) %}
{% endmacro %}

{% materialization snapshot, adapter='odps' %}
    {% set config = model['config'] %}
    {% set target_table = model.get('alias', model.get('name')) %}
    {% set strategy_name = config.get('strategy') %}
    {% set unique_key = config.get('unique_key') %}
    -- grab current tables grants config for comparision later on
    {% set grant_config = config.get('grants') %}
    {% set target_relation_exists, target_relation = get_or_create_relation(
            database=model.database,
            schema=model.schema,
            identifier=target_table,
            type='table')
    %}

    {% if not target_relation.is_table %}
        {% do exceptions.relation_wrong_type(target_relation, 'table') %}
    {% endif %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set strategy_macro = strategy_dispatch(strategy_name) %}
    {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

    {% set staging_table = odps_build_snapshot_staging_table(strategy, model['compiled_code'], target_relation, target_relation_exists) %}

    {% if not target_relation_exists %}
        {% call statement('create_table_like') %}
            {{ create_table_like(target_relation, staging_table) }}
        {% endcall %}

        {% set final_sql %}
        insert into table {{ target_relation }} select * from {{ staging_table }}
        {% endset %}
    {% else %}
        {{ adapter.valid_snapshot_target(target_relation) }}

        -- this may no-op if the database does not require column expansion
        {% do adapter.expand_target_column_types(from_relation=staging_table,
                                                to_relation=target_relation) %}

        {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                    | rejectattr('name', 'equalto', 'dbt_change_type')
                                    | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                    | rejectattr('name', 'equalto', 'dbt_unique_key')
                                    | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                    | list %}

        {% do create_columns(target_relation, missing_columns) %}

        {% set target_columns = adapter.get_columns_in_relation(target_relation) %}
        {% set quoted_target_columns = [] %}
        {% for column in target_columns %}
            {% do quoted_target_columns.append(column.quoted) %}
        {% endfor %}

        {% set final_sql = snapshot_merge_sql(
                target = target_relation,
                source = staging_table,
                insert_cols = quoted_target_columns
            )
        %}
    {% endif %}

    {% call statement('main') %}
        {{ final_sql }}
    {% endcall %}

    {% set should_revoke = should_revoke(target_relation_exists, full_refresh_mode=False) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(target_relation, model) %}

    {% if not target_relation_exists %}
        {% do create_indexes(target_relation) %}
    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {{ adapter.commit() }}

    {% if staging_table is defined %}
        {% do post_snapshot(staging_table) %}
    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
