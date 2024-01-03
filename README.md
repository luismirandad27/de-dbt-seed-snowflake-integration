# Mastering CI with dbt Seeds featuring Snowflake

When we deal with external data sources such as csv or JSON files hosted in a Cloud Storage such as S3 or GCP buckets. In snowflake, we can leverage the data loading process by using ****stages****, in a way that we can create ****storage integrations**** and with few commands, we can get the data in our tables.

Using **dbt (data build tool)** with Snowflake offers several key benefits: it enables version control for data models, ensuring reliable and maintainable codebases; promotes modularity and reusability in data modeling; simplifies complex data transformations. 

One interesting feature is dbt seeds: these are csv files that dbt can load directly into your data warehouse, allowing for easy integration of static data into your dbt workflows. 

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

    