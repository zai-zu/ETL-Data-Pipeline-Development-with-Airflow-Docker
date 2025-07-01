
  
    

  create  table "airflow"."driven_staging"."dim_person__dbt_tmp"
  
  
    as
  
  (
    

WITH source_data AS (
    SELECT
        unique_id,
        person_name,               
        user_name,                 
        email,                     
        phone,
        birth_date,
        personal_number
    FROM "airflow"."driven_raw"."raw_batch_data"
)

SELECT *
FROM   source_data
  );
  