

WITH src AS (
    SELECT
        fnu.unique_id,
        df.iban,
        fnu.download_speed,
        fnu.upload_speed,
        fnu.session_duration,
        fnu.consumed_traffic,
        ( (fnu.download_speed + fnu.upload_speed + 1) / 2 )
          + ( fnu.consumed_traffic / NULLIF(fnu.session_duration,0) )
          AS payment_amount
    FROM "airflow"."driven_staging"."fact_network_usage" fnu
    JOIN "airflow"."driven_staging"."dim_finance" df USING (unique_id)
)

SELECT * FROM src