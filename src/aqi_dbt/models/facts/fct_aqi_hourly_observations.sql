{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "observed_at",
            "data_type": "timestamp",
            "granularity": "month",
        },
        cluster_by=["site_id", "observed_date"],
        unique_key="id",
    )
}}

with
    stg_aqi_hourly_observations as (
        select * from {{ ref("stg_aqi_hourly_observations") }}
    ),
    dim_sites as (select * from {{ ref("dim_sites") }}),

    grouped as (
        select
            stg_aqi_hourly_observations.observed_at,
            stg_aqi_hourly_observations.observed_date,
            stg_aqi_hourly_observations.site_id,

            array_agg(
                struct(
                    stg_aqi_hourly_observations.site_id,
                    stg_aqi_hourly_observations.observed_at,
                    stg_aqi_hourly_observations.parameter_name,
                    stg_aqi_hourly_observations.reporting_units,
                    stg_aqi_hourly_observations.observed_value,
                    stg_aqi_hourly_observations.observed_aqi
                )
            ) as observations,

            max(stg_aqi_hourly_observations.observed_aqi) as observed_aqi,

        from stg_aqi_hourly_observations

        where
            1 = 1
            {% if is_incremental() -%}
            and observed_at > (select max(observed_at) from {{ this }})
            {%- endif %}

        group by 1, 2, 3
    ),

    joined as (
        select
            {{ dbt_utils.surrogate_key(["grouped.site_id", "grouped.observed_at"]) }}
            as id,

            grouped.observed_at,
            grouped.observed_date,
            grouped.site_id,

            struct(
                grouped.site_id,
                dim_sites.site_name,
                dim_sites.data_source_agency,
                dim_sites.geo,
                dim_sites.elevation,
                dim_sites.epa_region,
                dim_sites.country_code,
                dim_sites.msa_code,
                dim_sites.msa_name,
                dim_sites.state_code,
                dim_sites.state_name,
                dim_sites.county_code,
                dim_sites.county_name
            ) as site_data,

            grouped.observations,
            grouped.observed_aqi,

        from grouped
        -- this filters out ~700k records from 29 site_ids that are not
        -- currently in dim_sites
        join dim_sites on grouped.site_id = dim_sites.site_id
    )

select *
from joined
