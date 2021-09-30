{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            'field': 'observed_at', 
            'data_type': 'timestamp',
            'granularity': 'month'
        },
        cluster_by = ['site_id', 'parameter_name', 'observed_date'],
        unique_key = 'id',
    )
}}

with
    source_table as (select * from {{ source('aqi_raw_data', 'aqi_hourly_observations') }}),
    renamed as (

        select
            {{ 
                dbt_utils.surrogate_key(['observed_at', 'site_id', 'parameter_name']) 
            }} as id,

            observed_at,
            extract(date from observed_at) as observed_date,

            site_id,
            site_name,
            data_source_agency,

            parameter_name,
            reporting_units,
            value as observed_value,

        from source_table

        where
            1=1
            {% if is_incremental() -%}
            and observed_at > (select max(observed_at) from {{ this }})
            {%- endif %}

        -- there are about 56k duplicates in the raw data
        qualify row_number() over (partition by observed_at, site_id, parameter_name) = 1

    )

select * from renamed