{% macro copy_staged_files_into_tables(node, schema_name = 'landing') -%}

    -- depends_on: {{ ref('seed_snowflake_stages') }}
    -- depends_on: {{ ref('seed_snowflake_raw_table_columns') }}

    {# Create the array of storage stages for the cloud storage provider  #}
    {%- set env= env_var('DBT_ENV_NAME') -%}
    {%- set database_name = 'db_' ~ env ~ '.' ~ schema_name -%}

    {# Creating the Storage Integration #}
    {%- set query -%}
        select * from {{ ref('seed_snowflake_stages') }} where flg_active = 1 and flg_move_data = 1;
    {%- endset -%}

    {%- set res_snowflake_stages = run_query(query) -%}

    {# Create the Snowflake Stages #}
    {%- for row in res_snowflake_stages -%}

        {%- set id = row[0] -%}

        {%- set file_format = row[3] -%}

        {%- set stage_name = {{ schema_name }} ~ '_stage_' ~ row[2] ~ '_' ~ row[3] ~ '_' ~ env ~ '_' ~ row[4] -%}

        {%- set table_name = row[1] ~ '_' ~ row[2] ~ '_' ~ row[3] ~ '_' ~ row[4] -%}

        {{ log('Copying table ' ~ table_name ~ '...')  }}

        {%- if file_format == 'json' -%}

            {# The data is an array of JSON objects? #}
            {%- set has_outer_array = row[6]-%}

            {%- set drop_file_format -%}
                drop file format if exists {{ database_name }}.json_format_array_temp_{{row[4]}}
            {%- endset -%}

            {%- set temp_file_format = run_query(drop_file_format) -%}

            {# Create an json format adhoc for this copy procedure #}
            {%- set create_file_format -%}
                create file format {{ database_name }}.json_format_array_temp_{{row[4]}}
                type = 'JSON'
                {%- if has_outer_array == 1 -%}
                    strip_outer_array = true
                {%- endif -%}    
            {%- endset -%}

            {%- set temp_file_format = run_query(create_file_format) -%}
            
            {%- set copy_into_data -%}
                {# Run copy into #}
                copy into {{ database_name }}.{{ table_name }} (
                    metadata_filename,
                    metadata_file_row_number,
                    metadata_file_content_key,
                    metadata_file_last_modified,
                    metadata_start_scan_time,
                    json_object,
                    updated_at
                )
                from
                    (
                        SELECT
                            METADATA$FILENAME as metadata_filename,
                            METADATA$FILE_ROW_NUMBER as METADATA_FILE_ROW_NUMBER,
                            METADATA$FILE_CONTENT_KEY as METADATA_FILE_CONTENT_KEY,
                            METADATA$FILE_LAST_MODIFIED as METADATA_FILE_LAST_MODIFIED,
                            METADATA$START_SCAN_TIME as METADATA_START_SCAN_TIME,
                            t.$1,
                            current_timestamp()
                        FROM
                            @{{ database_name }}.{{ stage_name }} (file_format => {{ database_name }}.json_format_array_temp_{{row[4]}} ) t
                    );
            {%- endset -%}

            {%- set copy_into_temp = run_query(copy_into_data) -%}

            {%- set drop_file_format -%}
                drop file format if exists {{ database_name }}.json_format_array_temp_{{row[4]}}
            {%- endset -%}

            {%- set temp_file_format = run_query(drop_file_format) -%}

            {{ log('USER LOG: Table '~ table_name ~ ' updated.')}}
            
        {%- elif file_format == 'csv' -%}

            {# Delimiter #}
            {%- set delimiter = row[5]-%}

            {# The data is an array of JSON objects? #}
            {%- set has_outer_array = row[6]-%}

            {%- set drop_file_format -%}
                drop file format if exists {{ database_name }}.csv_format_array_temp_{{row[4]}}
            {%- endset -%}

            {%- set temp_file_format = run_query(drop_file_format) -%}

            {# Create an csv format adhoc for this copy procedure #}
            {%- set create_file_format -%}
                create file format {{ database_name }}.csv_format_array_temp_{{row[4]}}
                type = 'CSV'
                field_delimiter = '{{ delimiter }}'
                record_delimiter = '\n'
                skip_header = 1
                field_optionally_enclosed_by = '"'
            {%- endset -%}

            {%- set temp_file_format = run_query(create_file_format) -%}

            {# Query to list all columns for a table #}
            {%- set query_table_columns -%}

                select
                    listagg( '$' || column_position || ', ') within group (order by column_position) as concatenated_columns
                from
                    {{ ref('seed_snowflake_raw_table_columns') }}
                where 
                    id = '{{ id }}'

            {%- endset -%}

            {%- set res_snowflake_table_cols = run_query(query_table_columns) -%}

            {%- for row_col in res_snowflake_table_cols -%}

                {# Create query #}
                {%- set copy_stage_data -%}

                    copy into {{ database_name }}.{{ table_name }} 
                    from
                        (
                            SELECT
                                METADATA$FILENAME as metadata_filename,
                                METADATA$FILE_ROW_NUMBER as METADATA_FILE_ROW_NUMBER,
                                METADATA$FILE_CONTENT_KEY as METADATA_FILE_CONTENT_KEY,
                                METADATA$FILE_LAST_MODIFIED as METADATA_FILE_LAST_MODIFIED,
                                METADATA$START_SCAN_TIME as METADATA_START_SCAN_TIME,
                                {{row_col[0]}}
                                current_timestamp()
                            FROM
                                @{{ database_name }}.{{ stage_name }} (file_format => {{ database_name }}.csv_format_array_temp_{{row[4]}} ) t
                        );

                {%- endset -%}

                {%- set res_snowflake_stage_copy = run_query(copy_stage_data) -%}

            {%- endfor -%}

            {%- set drop_file_format -%}
                drop file format if exists {{ database_name }}.csv_format_array_temp_{{row[4]}}
            {%- endset -%}

            {%- set temp_file_format = run_query(drop_file_format) -%}

            {{ log('USER LOG: Table '~ table_name ~ ' updated.')}}

        {%- else -%}

            {{ log('USER LOG: Table '~ table_name ~ ' not updated.')}}

        {%- endif -%}

    {% endfor %}

    {{ log("USER LOG: Deployment Snowflake Data Cloud - Migrating Data - FINISHED") }}

{% endmacro %}