{{
    config(
        materialized='incremental',
        strategy='delete+insert',
        unique_key='row_id',
        on_schema_change='append_new_columns',
        postgres_partition='partition_field'
    )
}}

select 
row_id,
partition_field,
created_at,
updated_at
from {{ ref('stg_public__latest_buoy_reports_for_m6') }}