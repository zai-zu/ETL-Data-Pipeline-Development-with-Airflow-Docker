
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