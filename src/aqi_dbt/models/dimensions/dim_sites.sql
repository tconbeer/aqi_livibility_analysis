{{
    config(
        materialized="table",
        cluster_by=["site_id"],
    )
}}

with
    stg_aqi_daily_sites as (select * from {{ ref("stg_aqi_daily_sites") }}),
    stg_sites_msa_mapping as (select * from {{ ref("stg_sites_msa_mapping") }}),
    aggregated as (

        select distinct
            site_id,

            last_value(observed_date respect nulls) over (
                site_window
            ) as last_observed_date,

            last_value(site_name respect nulls) over (site_window) as site_name,

            last_value(data_source_id respect nulls) over (
                site_window
            ) as data_source_id,
            last_value(data_source_agency respect nulls) over (
                site_window
            ) as data_source_agency,

            last_value(latitude respect nulls) over (site_window) as latitude,
            last_value(longitude respect nulls) over (site_window) as longitude,
            last_value(elevation respect nulls) over (site_window) as elevation,

            last_value(epa_region respect nulls) over (site_window) as epa_region,
            last_value(country_code respect nulls) over (site_window) as country_code,
            last_value(msa_code respect nulls) over (site_window) as msa_code,
            last_value(msa_name respect nulls) over (site_window) as msa_name,
            last_value(state_code respect nulls) over (site_window) as state_code,
            last_value(state_name respect nulls) over (site_window) as state_name,
            last_value(county_code respect nulls) over (site_window) as county_code,
            last_value(county_name respect nulls) over (site_window) as county_name,

        from stg_aqi_daily_sites
        where
            observed_date is not null
            and parameter_name is not null
            and site_name is not null
            and latitude is not null
            and longitude is not null
            and country_code is not null

        window
            site_window as (
                partition by site_id
                order by observed_date asc
                rows between unbounded preceding and unbounded following
            )
    ),

    joined as (

        select
            aggregated.site_id,
            aggregated.last_observed_date,
            aggregated.site_name,
            aggregated.data_source_id,
            aggregated.data_source_agency,
            st_geogpoint(aggregated.longitude, aggregated.latitude) as geo,
            aggregated.elevation,
            aggregated.epa_region,
            aggregated.country_code,
            coalesce(aggregated.msa_code, stg_sites_msa_mapping.msa_code) as msa_code,
            coalesce(aggregated.msa_name, stg_sites_msa_mapping.msa_name) as msa_name,
            aggregated.state_code,
            aggregated.state_name,
            aggregated.county_code,
            aggregated.county_name,

        from aggregated
        left join
            stg_sites_msa_mapping on aggregated.site_id = stg_sites_msa_mapping.site_id
    ),

    final as (select joined.*, st_geohash(geo, 8) as geohash, from joined)

select *
from final
