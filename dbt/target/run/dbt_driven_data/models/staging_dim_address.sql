
  
    

  create  table "airflow"."driven_staging"."dim_address__dbt_tmp"
  
  
    as
  
  (
    
WITH source_data AS (
 SELECT
 unique_id,
 address,
 mac_address,
 ip_address
 FROM
 "airflow"."driven_raw"."raw_batch_data"
)
SELECT
 *
FROM
 source_data
  );
  