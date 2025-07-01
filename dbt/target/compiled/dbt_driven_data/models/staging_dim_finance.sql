
WITH source_data AS (
 SELECT
 unique_id,
 iban
 FROM
 "airflow"."driven_raw"."raw_batch_data"
)
SELECT
 *
FROM
 source_data