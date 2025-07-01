{{ config(
    materialized='table',
    schema='trusted',
    alias='technical_data',
    tags=['trusted']
) }}

WITH src AS (
    SELECT
        fnu.unique_id,
        da.address,
        da.mac_address,
        da.ip_address,
        fnu.download_speed,
        fnu.upload_speed,
        ROUND(fnu.session_duration / 60.0, 1)             AS min_session_duration,
        CASE
            WHEN fnu.download_speed   < 50
              OR fnu.upload_speed     < 30
              OR fnu.session_duration < 60
            THEN true ELSE false
        END                                               AS technical_issue
    FROM {{ source('staging_source', 'fact_network_usage') }} fnu
    JOIN {{ source('staging_source', 'dim_address') }} da USING (unique_id)
)

SELECT * FROM src

