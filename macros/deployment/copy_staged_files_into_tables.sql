{% macro copy_staged_files_into_tables(node) -%}

    -- depends_on: {{ ref('seed_snowflake_stages') }}
    -- depends_on: {{ ref('seed_snowflake_raw_table_columns') }}

    {# Create the array of storage stages for the cloud storage provider  #}
    {%- set env= env_var('DBT_ENV_NAME') -%}
    {%- set database_name = 'db_' ~ env -%}

    {# Creating the Storage Integration #}
    {%- set query -%}
        select * from {{ ref('seed_snowflake_stages') }} where flg_move_data = 1;
    {%- endset -%}

    {%- set res_snowflake_stages = run_query(query) -%}

    {# Create the Snowflake Stages #}
    {%- for row in res_snowflake_stages -%}

        {%- set file_format = row[2] -%}

        {%- set stage_name = 'landing_stage_' ~ storage_prov ~ '_' ~ row[2] ~ '_' ~ env ~ '_' ~ row[3] -%}

        {%- set table_name = row[0] ~ '_' ~ row[1] ~ '_' ~ row[2] ~ '_' ~ row[3] -%}

        {%- log('Copying table ' ~ table_name ~ '...')  -%}

        {%- if file_format == 'json' -%}

        {%- elif file_format = 'csv' -%}

        {%- else -%}

        {%- endif -%}

        {# Query to list all columns for a table #}
        {%- set query_information_schema -%}

            select
                listagg( '$' || column_position || ', ') within group (order by column_position) as concatenated_columns
            from
                {{ ref('seed_snowflake_raw_table_columns') }}
            where 
                id = '{{ id }}'

        {%- endset -%}

        {%- set res_snowflake_table_cols = run_query(query_information_schema) -%}

        {# Create query #}
        {%- set copy_stage_query -%}

            copy into {{database_name}}.landing.{{table_name}}({{columns_str_schema}},updated_at)
            from (
                select {{columns_str_select}},current_timestamp()
                from @{{database_name}}.landing.{{stage_name}}
            )
            on_error = 'continue'
            file_format = {{database_name}}.landing.{{storage_prov}}_{{file_format}}_format

        {%- endset -%}

        {%- set res_snowflake_stage_copy = run_query(copy_stage_query) -%}

    {% endfor %}

    [{{ cp }} - {{ env }} ] Deployment Snowflake Data Cloud - Migrating Data - SUCCESS

{% endmacro %}