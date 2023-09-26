

with 

source as (

    select * from {{ source('public', 'latest_buoy_reports_for_m6') }}

),

renamed as (

    select
        windgustdir,
        temp,
        period,
        windgust,
        seatemp,
        created_at::timestamp,
        winddir,
        pressure,
        dewpoint,
        wavedir,
        wmoid,
        reportdate,
        updated_at::timestamp,
        TO_CHAR(created_at::timestamp, 'YYYYMMDD')::int as partition_field,
        name,
        humidity,
        time,
        windspeed,
        height,
        stationid,
        reporttime,
        _airbyte_ab_id as row_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_latest_buoy_reports_for_m6_hashid

    from source

)

select * from renamed
