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
    stg_aqi_hourly_observations as (select * from {{ ref('stg_aqi_hourly_observations') }}),
    dim_sites as (select * from {{ ref('dim_sites') }}),
    joined as (
        
        select
            stg_aqi_hourly_observations.id,

            stg_aqi_hourly_observations.observed_at,
            stg_aqi_hourly_observations.observed_date,

            stg_aqi_hourly_observations.site_id,
            dim_sites.site_name,
            dim_sites.data_source_agency,
            dim_sites.latitude,
            dim_sites.longitude,
            dim_sites.elevation,
            dim_sites.epa_region,
            dim_sites.country_code,
            dim_sites.msa_code,
            dim_sites.msa_name,
            dim_sites.state_code,
            dim_sites.state_name,
            dim_sites.county_code,
            dim_sites.county_name,

            stg_aqi_hourly_observations.parameter_name,
            stg_aqi_hourly_observations.reporting_units,
            stg_aqi_hourly_observations.observed_value,
            {{ convert_obs_to_aqi(
                "stg_aqi_hourly_observations.parameter_name",
                "stg_aqi_hourly_observations.reporting_units",
                "stg_aqi_hourly_observations.observed_value",
            ) }} as observed_aqi,

        from
            stg_aqi_hourly_observations
            -- this filters out ~700k records from 29 site_ids that are not
            -- currently in dim_sites
            join dim_sites on stg_aqi_hourly_observations.site_id = dim_sites.site_id

    )

select * from joined