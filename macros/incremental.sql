{% macro snowflake__get_incremental_manual_dml_sql(arg_dict) %}
    {%- set dest_columns = arg_dict.dest_columns -%}
    {%- set temp_relation = arg_dict.temp_relation -%}
    {%- set target_relation = arg_dict.target_relation -%}
    {%- set timestamp_column = config.get('timestamp_column') -%}
    {%- set exclude_columns = config.get('exclude_columns') -%}
    {%- set driver_key = config.require('driver_key',) %}


    BEGIN TRANSACTION;

    -- Delete records not present in source
    DELETE FROM {{ target_relation }} AS dest
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ temp_relation }} AS src
        WHERE 
        {% for driver_column in driver_key %}
        dest.{{ driver_column }} = src.{{ driver_column }}
        {% if not loop.last %} AND {% endif %}
        {% endfor %}
    );

    -- Update only when unique_key matches and hashes differ
    UPDATE {{ target_relation }} AS dest
    SET
        {% for column in dest_columns if column.name not in ['hash_key', timestamp_column] %}
         {{ column.name }} = src.{{ column.name }}{% if not loop.last %},{% endif %}
        
        {% endfor %}

    FROM {{ temp_relation }} AS src
    WHERE {% for driver_column in driver_key %}
        dest.{{ driver_column }} = src.{{ driver_column }}
        {% if not loop.last %} AND {% endif %}
        {% endfor %}
      AND dest.hash_key IS DISTINCT FROM src.hash_key;

    -- Insert new records where unique_key doesn't exist
    INSERT INTO {{ target_relation }} (
        {% for column in dest_columns %}
        {{ column.name }} {% if not loop.last %},{% endif %}
        {% endfor %}
    )
    SELECT 
        {% for column in dest_columns %}
        src.{{ column.name }} {% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ temp_relation }} AS src
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ target_relation }} AS dest
        WHERE 
        {% for driver_column in driver_key %}
        dest.{{ driver_column }} = src.{{ driver_column }}
        {% if not loop.last %} AND {% endif %}
        {% endfor %}
    );

    COMMIT;
{% endmacro %}

{% materialization incremental, adapter='snowflake' -%}
    {%- set driver_key = config.require('driver_key',) %}
    {%- set incremental_strategy = config.get('incremental_strategy', 'default') -%}
    {%- set timestamp_column = config.get('timestamp_column') -%}
    {%- set compiled_sql = model['compiled_code'] -%}


    {#- Validate mandatory configurations -#}
    {% if incremental_strategy == 'manual_dml' %}
        {% if not driver_key %}
            {% do exceptions.raise_compiler_error("manual_dml strategy requires driver_key configuration") %}
        {% endif %}
        {% if not timestamp_column %}
            {% do exceptions.raise_compiler_error("manual_dml strategy requires timestamp_column configuration") %}
        {% endif %}
    {% endif %}

    {# Get columns from compiled SQL #}
    {%- set columns_query = "SELECT * FROM (" ~ compiled_sql ~ ") WHERE 1=0" %}
    {%- set columns_result = run_query(columns_query) %}
    {%- set source_columns = columns_result.columns | map(attribute="name") %}
   {%- set hash_columns = source_columns | reject('in', exclude_columns) | list -%}
--if columns_result else []

    {%- set target_relation = this.incorporate(type='table') -%}
    {%- set existing_relation = load_relation(target_relation) -%}
    {%- set tmp_relation = make_temp_relation(target_relation) -%}

    {{ run_hooks(pre_hooks) }}

    {% if existing_relation is none or should_full_refresh() %}
        {#- Initial load or full refresh -#}
        {%- call statement('main') -%}
            {{ create_table_as(False, target_relation, compiled_sql) }}
        {%- endcall -%}
    {% else %}
        {#- Create temp table with hash generation -#}
        {%- call statement('create_tmp_relation') -%}
            CREATE OR REPLACE TEMPORARY TABLE {{ tmp_relation }} AS (
                SELECT
                    *,
                    {{ dbt_utils.generate_surrogate_key(hash_columns) }} AS hash_key
                FROM ({{ compiled_sql }}) AS base
            );
        {%- endcall -%}

        {#- Handle schema changes -#}
        {% set dest_columns = process_schema_changes(config.get('on_schema_change'), tmp_relation, existing_relation) %}
        {% if not dest_columns %}
            {% set dest_columns = adapter.get_columns_in_relation(tmp_relation) %}
        {% endif %}

        {#- Execute incremental strategy -#}
        {%- call statement('main') -%}
            {{ snowflake__get_incremental_manual_dml_sql({
                'target_relation': target_relation,
                'temp_relation': tmp_relation,
                'unique_key': driver_key,
                'dest_columns': dest_columns
            }) }}
        {%- endcall -%}
    {% endif %}

    {{ run_hooks(post_hooks) }}
    {% do persist_docs(target_relation, model) %}

    {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}