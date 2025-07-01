
  
    

  create  table "airflow"."driven_staging"."fact_network_usage__dbt_tmp"
  
  
    as
  
  (
    
WITH source_data AS (
 SELECT
 unique_id,
 session_duration,
 download_speed,
 upload_speed,
 consumed_traffic
 FROM
 "airflow"."driven_raw"."raw_batch_data"
)
SELECT
 *
FROM
 source_data
  );
  