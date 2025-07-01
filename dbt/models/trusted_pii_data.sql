{{ config(
    materialized='table',
    schema='trusted',
    alias='pii_data',
    tags=['trusted']
) }}

WITH src AS (
    SELECT
        dp.person_name            AS person_name,
        dp.user_name                    AS user_name,
        dp.email,
        dp.personal_number,
        dp.birth_date,
        da.address,
        dp.phone,
        da.mac_address,
        da.ip_address,
        df.iban,
        dd.accessed_at,
        fnu.session_duration,
        fnu.download_speed,
        fnu.upload_speed,
        fnu.consumed_traffic,
        fnu.unique_id
    FROM {{ source('staging_source', 'fact_network_usage') }} fnu
    JOIN {{ source('staging_source', 'dim_address') }}  da USING (unique_id)
    JOIN {{ source('staging_source', 'dim_date') }}     dd USING (unique_id)
    JOIN {{ source('staging_source', 'dim_finance') }}  df USING (unique_id)
    JOIN {{ source('staging_source', 'dim_person') }}   dp USING (unique_id)
)

SELECT * FROM src

