# Mastering CI with dbt Seeds featuring Snowflake

On this repository, we can leverage the application of dbt seeds in a way that we can configure and optimize the CI process to maintain our Snowflake stages and efficiently update our raw data.

Some key benefits of this project:

- Automatically update the **snowflake storage integrations with** different cloud storage providers (S3, GCP Cloud Storage, Azure Blob Storage).
- Maintain Snowflake stages dynamically.
- Create tables that will receive the raw data from the external data sources.
- If we have data that only needs to be loaded once, this dbt project contains a macro that copy the data from csv or JSON files.

### If you want to use it, you need to complete the following requirements:

1. You need to have your dbt project already set (obviously) with an integration with GitHub and Snowflake.
2. Make a few adjustment on your **dbt_project.yml** to allow using special characters on the dbt seed (specially for the comma):
    
    ```yaml
    seeds:
      <your-project-name>:
        +delimiter: ";"
    ```
    
3. The following are the seeds you need to create:

    **seed_snowflake_stages**
    
    Contains the list of the stages, dbt will maintain on every CI job. The following table shows the seed’s columns:

    | Column                 | Description                                                                                                                                                 |
    |------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | id                     | Id of the stage. Should be unique                                                                                                                           |
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

    For csv data sources, this seed table will have the list of columns for each table source: The following table shows the seed’s columns:

    | Column          | Description                                                                   |
    |-----------------|-------------------------------------------------------------------------------|
    | id              | id of the table. It should be linked to the id column from the previous seed. |
    | column_position | Position number of the column in the snowflake table.                         |
    | column_name     | Name of the column in the snowflake table.                                    |

### **Use cases for this demo**

Once you have your dbt project set with the seeds you can use the following macro based on each use case described. Don’t forget to copy the macros on the **/macros** folder.

1. **Create Snowflake Stages dynamically**
    
    The macro ***deploy_snowflake_stages*** will update the storage integration with an specific cloud storage provider. It will create (or recreate) the snowflake stages according to the ******************************************seed_snowflake_stages****************************************** seed with *flg_active* 1.
    
    ```bash
    dbt run-operation deploy_snowflake_stages --args '{storage_prov: aws}'
    ```
    
2. **Create Snowflake tables associated to Stages**
    
    The ***deploy_snowflake_raw_tables*** macro that will create tables in the snowflake schema related to the landing zone of the Data Cloud architecture. It will create (or recreate) the tables according to the ******************************************seed_snowflake_stages****************************************** seed with *flg_active* 1. For csv files, the tables are going to contain the columns referred in the ********************************seed_snowflake_raw_tables_columns******************************** seed.
    
    ```bash
    dbt run-operation deploy_snowflake_raw_tables --args '{storage_prov: aws}'
    ```
    
3. **Copy data from Snowflake stages to tables automatically**
    
    The ***copy_staged_files_into_tables*** will help you on that. Based on the schema defined in the parameters, it will look for all tables that should be updated in the deployment (CI) job process based on the *************flg_move_data************* column in the ************seed_snowflake_stages************ seed.
    
    ```bash
    dbt run-operation copy_staged_files_into_tables --args '{schema: landing}'
    ```
    

*****If everything works fine, add this instructions (including the dbt seed --select …) into your CI job for your next deployments!*****

### Future Work:

This is an initial work that it’s already working where I’m working, feel free to propose more things to improve or cover more casuistic to make this process more robust. 

Some other things that I have in my mind are:

- Allowing data loading from Avro and Parquet files.
- Testing this process with AWS and Azure storage services.