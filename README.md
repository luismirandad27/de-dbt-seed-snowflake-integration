# Mastering CI with dbt Seeds featuring Snowflake

This repository offers an innovative approach to applying dbt seeds, specifically tailored for enhancing and fine-tuning the CI processes. It focuses on effectively managing Snowflake stages and streamlining the updating of raw data.

Highlighted advantages of this initiative include:

- Seamless synchronization of **Snowflake storage integrations** with various cloud storage services (S3, GCP Cloud Storage, Azure Blob Storage).
- Dynamic management of **Snowflake stages**.
- Establishment of tables designated for receiving **raw data** from diverse external data sources.
- Inclusion of a specialized dbt macro for one-time data loading, accommodating data from CSV or **JSON** files.

### If you want to use it, you need to follow these requirements:

1. Firstly, ensure that your dbt project is already established, with integrations in place for both GitHub and Snowflake.
2. On snowflake, you might have 3 databases:
    - `db_dev`: environment where you can test your dbt pipelines
    - `db_qa`: environment where you can test your deployment
    - `db_prod`: environment for your final pipelines.
    
    If you want to use another name, you can change the prefix, but I suggest to keep the environment name (dev, qa, prod) due we can use environmental variables on dbt cloud to manage the deployments.
3. For raw data, create an schema on each database called `landing`. This schema will store all the tables connected to external sources, you can use another name instead.
4. On each database create a **storage integration** with the specific cloud storage provider. On this repository, I'm connecting with AWS S3 Buckets

    ```sql
        CREATE STORAGE INTEGRATION storage_integration_aws_{environment}
        TYPE = EXTERNAL_STAGE
        STORAGE_PROVIDER = 'S3'
        ENABLED = TRUE
        STORAGE_AWS_ROLE_ARN = '{your_role_arn}'
        STORAGE_ALLOWED_LOCATIONS = ('s3://dummy-bucket/');
    ```
    To make this storage integration working with this repository, the allowed values for the {environment} suffix are: dev, qa and prod. Finally, to enable the communication between your snowflake account and the AWS S3 Buckets, you can follow
    this link: [Link S3 with Storage Integration](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration)

5. Modify your dbt_project.yml file to enable the use of special characters in dbt seed, particularly focusing on accommodating the comma character:
    
    ```yaml
    seeds:
      <your-project-name>:
        +delimiter: ";"
    ```
    
6. The following are the seeds you need to create:

    **seed_snowflake_stages**
    
    Contains the list of the snowflake stages dbt will maintain on every CI job (deployment). The following table show the seed’s columns:

    | Column                 | Description                                                                                                                                                 |
    |------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | id                     | Id of the stage. Should be unique.                                                                                                                         |
    | type_stage             | Type of data to be staged. You can customize in a manner you would understand the purpose of the data. In this case, due it’s raw data, the value is “raw”. |
    | cloud_storage_provider | Values allowed: aws, gcp, azure.                                                                                                                            |
    | source_file_type       | File format file. Values allowed: csv, json.                                                                                                                |
    | stage_name_suffix      | Suffix for the name of the table and stage.                                                                                                                 |
    | delimiter              | Delimiter character (csv files only)                                                                                                                        |
    | flg_strip_array        | Json files only. If the file is an array of JSON object the value should be 1.                                                                              |
    | flg_active             | If the stage should remain active for the next deployment, keep the value as 1.                                                                             |
    | flg_move_data          | If the data should be copy to the snowflake table in the deployment process, keep the value as 1.                                                           |
    | flg_recreate           | If you need to re-update the stage (change the url) or the table (columns), keep the value as 1.                                                            |

    **seed_snowflake_raw_table_columns**

    For csv data sources, this seed table will have the list of columns for each table source. The following table shows the seed’s columns:

    | Column          | Description                                                                   |
    |-----------------|-------------------------------------------------------------------------------|
    | id              | id of the table. It should be linked to the id column from the previous seed. |
    | column_position | Position number of the column in the snowflake table.                         |
    | column_name     | Name of the column in the snowflake table.                                    |

### **Use cases for this demo**

After setting up your dbt project with the necessary seeds, you can apply the provided macro tailored to each specific use case mentioned. Remember to place the macros in the **/macros** directory.

1. **Create Snowflake Stages dynamically**
    
    The macro ***deploy_snowflake_stages*** will update the storage integration for an specific cloud storage provider. It will create (or recreate) the snowflake stages according to the **seed_snowflake_stages** seed with *flg_active* 1.
    
    ```bash
    dbt run-operation deploy_snowflake_stages --args '{storage_prov: aws, landing_schema_name: landing}'
    ```
    
2. **Create Snowflake tables associated to Stages**
    
    The ***deploy_snowflake_raw_tables*** macro will create tables in the snowflake schema for raw data of the Data Cloud architecture. It will create (or recreate) the tables according to the **seed_snowflake_stages** seed with *flg_active* 1. For csv files, the tables are going to contain the columns referred in the **seed_snowflake_raw_tables_columns** seed.
    
    ```bash
    dbt run-operation deploy_snowflake_raw_tables --args '{storage_prov: aws, landing_schema_name: landing}'
    ```
    
3. **Copy data from Snowflake stages to tables automatically**
    
    The ***copy_staged_files_into_tables*** will help you on that. Based on the schema defined in the parameters, it will look for all tables that should be updated in the deployment (CI) job process based on the **flg_move_data** column in the **seed_snowflake_stages** seed.
    
    ```bash
    dbt run-operation copy_staged_files_into_tables --args '{schema_name: landing}'
    ```
    

*****If everything works fine, add this instructions (including the dbt seed --select …) into your CI job for your next deployments!*****

### Future Work:

This is an initial work that it’s already working on my current job, feel free to propose more things to improve or cover more casuistic to make this pipeline more robust. 

Some other things that I have in my mind are:

- Allowing data loading from Avro and Parquet files.
- Testing this process with AWS and Azure storage services.