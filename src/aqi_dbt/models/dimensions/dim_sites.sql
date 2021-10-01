{{
    config(
        materialized = 'table',
        cluster_by = ['site_id'],
    )
}}

with
    stg_aqi_daily_sites as (select * from {{ ref('stg_aqi_daily_sites') }}),
    aggregated as (

        select distinct
            site_id,

            last_value(observed_date respect nulls) over (site_window) as last_observed_date,

            last_value(site_name respect nulls) over (site_window) as site_name,
            
            last_value(data_source_id respect nulls) over (site_window) as data_source_id,
            last_value(data_source_agency respect nulls) over (site_window) as data_source_agency,
            
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

    final as (

        select
            aggregated.*,
            st_geogpoint(aggregated.longitude, aggregated.latitude) as geo
        
        from aggregated

    )

select * from final