{% macro deploy_snowflake_raw_tables(node, storage_prov='aws') -%}
    
    -- depends_on: {{ ref('seed_snowflake_raw_table_columns') }}

    {%- set database_name = 'db_' ~ env_var('DBT_ENV_NAME') -%}
    {%- set schema = 'landing'-%}

    {# Getting the list of sources to create #}
    {%- set query -%}
        select 
            id,
            source_file_type,
            type_stage || '_' || cloud_storage_provider || '_' || source_file_type || '_' || stage_name_suffix as table_name
        from {{ ref('seed_snowflake_stages') }} 
        where   cloud_storage_provider = '{{ storage_prov }}' and 
                flg_recreate = 1
    {%- endset -%}

    {%- set res_snowflake_tables = run_query(query) -%}

    {# Create the Snowflake Tables #}
    {%- for row in res_snowflake_tables -%}

        {%- set id = row[0] -%}
        {%- set source_file_type = row[1] -%}
        {%- set table_name = row[2] -%}

        {# Re-create table query #}
        {%- set drop_table -%}
            drop table if exists {{database_name}}.landing.{{table_name}}
        {%- endset -%}

        {%- set res_snowflake_stage_drop = run_query(drop_table) -%}

        {%- if source_file_type == 'json' -%}

            {%- set create_raw_table -%}
                
                {# It's not required to get the list of columns #}
                create or replace table {{database_name}}.landing.{{table_name}}
                (   
                    metadata_filename text,
                    metadata_file_row_number number,
                    metadata_file_content_key text,
                    metadata_file_last_modified timestamp,
                    metadata_start_scan_time timestamp,
                    json_object variant,
                    updated_at timestamp
                )

            {%- endset -%}

            {%- set temp = run_query(create_raw_table) -%}

            {{ log('USER LOG: Creating table ' ~ table_name ~ '...') }}

        {%- elif  source_file_type == 'csv' -%}

            {%- set query -%}

                select
                    listagg(column_name || ' string', ', ') within group (order by column_position) as concatenated_columns
                from
                    {{ ref('seed_snowflake_raw_table_columns') }}
                where
                    id = '{{ id }}'

            {%- endset -%}

            {%- set columns_csv = run_query(query) -%}

            {%- for row in columns_csv -%}

                {%- set str_columns = row[0] -%}

                {%- set create_table -%}

                    create or replace table {{database_name}}.landing.{{table_name}}
                    (   
                        metadata_filename text,
                        metadata_file_row_number number,
                        metadata_file_content_key text,
                        metadata_file_last_modified timestamp,
                        metadata_start_scan_time timestamp,
                        {{ str_columns }},
                        updated_at timestamp
                    )

                {%- endset -%}

                {%- set temp = run_query(create_table) -%}

            {%- endfor -%}

            {{ log('USER LOG: Creating table ' ~ table_name ~ '...') }}

        {%- else -%}

            {{ log('USER LOG: Not valid source file type')}}
        
        {%- endif -%}

    {% endfor %}

    {{ log("USER LOG:  Deployment Snowflake Data Cloud - Creating Tables - FINISHED") }}

{%- endmacro %}
