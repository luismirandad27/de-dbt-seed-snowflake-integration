{% macro deploy_snowflake_stages(node, storage_prov='aws') -%}
    
    -- depends_on: {{ ref('seed_snowflake_stages') }}
    
    {# Important Parameters #}
    {%- set env= env_var('DBT_ENV_NAME') -%}                {# Databse environment (dev, qa, prod) #}
    {%- set cloud_location=[] -%}                           {# List of bucket folder URLs #}
    {%- set database_name = 'db_' ~ env ~ '.landing.' -%}   {# Database and schema (landing) #}
    {%- set storage_prefix = '' -%}                         {# For AWS, adding the ARN of the bucket role #}

    {% if storage_prov == 'aws' %}
        {%- set storage_prefix = 's3' -%}
        {%- set bucket_name = 's3://snowflake-data-repository-'~env~'/'  -%}
    {% else %}
        {# Include your other buckets from other cloud providers #}
    {% endif %}

    {% set storage_prov_list = ['aws', 'gcp', 'azure'] %}

    {% if storage_prov in storage_prov_list %}
        
        {# ################################ #}
        {# Creating the Storage Integration #}
        {# ################################ #}
        {%- set storage_int_name = storage_prov ~ '_stg_int_' ~ env -%}

        {# Creating the Storage Integration #}
        {%- set query -%}
            select * from {{ ref('seed_snowflake_stages') }} where cloud_storage_provider = '{{ storage_prov }}' and flg_active = 1
        {%- endset -%}

        {%- set res_snowflake_stages = run_query(query) -%}

        {# Concatenating all the Bucket folders directories to STORAGE_ALLOWED_LOCATIONS parameter #}
        {%- for row in res_snowflake_stages -%}

            {%- set stage_name = "'" ~ bucket_name ~ row[1] ~ '_' ~ row[2] ~ '_' ~ row[3] ~ '_' ~ row[4] ~ '/' ~ "'" -%}
            {%- set tmp = cloud_location.append(stage_name) -%}

        {% endfor %}

        {%- set cloud_location_str = cloud_location | join(',\n') -%}

        {# Create the Snowflake Storage Integration #}
        {%- set update_stg_int -%}
            
            alter storage integration {{ storage_int_name }}
            set storage_allowed_locations = ({{ cloud_location_str }});

        {%- endset -%}

        {%- set result = run_query(update_stg_int) -%}

        {# ########################### #}
        {# Create the Snowflake Stages #}
        {# ########################### #}
        {%- for row in res_snowflake_stages -%}

            {%- set stage_name = 'landing_stage_' ~ storage_prov ~ '_' ~ row[3] ~ '_' ~ env ~ '_' ~ row[4] -%}

            {%- set drop_stage -%}
                drop stage if exists {{ database_name }}{{ stage_name }}
            {% endset %}

            {%- set result = run_query(drop_stage) -%}

            {%- set url_name = "'" ~ bucket_name ~ row[1] ~ '_' ~ row[2] ~ '_' ~ row[3] ~ '_' ~ row[4] ~ '/' ~ "'" -%}    

            {%- set create_stage -%}

                create stage {{ database_name }}{{stage_name}}
                url = {{ url_name }}
                storage_integration = {{ storage_int_name }}

            {% endset %}

            {%- set result = run_query(create_stage) -%}

        {% endfor %}

        {{ log("USER LOG: Deployment Snowflake Stages - FINISHED!") }}

    {% else %}

        {{ log("USER LOG: Deployment Snowflake Stages not executed! Invalid parameters!") }}

    {% endif %}

{%- endmacro %}