{{
    config(
        materialized = 'table',
        cluster_by = ['msa_code'],
    )
}}

with
    dim_sites as (select * from {{ ref('dim_sites') }}),

    aggregated as (

        select
            msa_code,
            any_value(msa_name) as msa_name,
            st_centroid_agg(geo) as msa_centroid,
            avg(elevation) as msa_elevation,
        
        from dim_sites
        where msa_code is not null
        group by 1

    ),

    final as (

        select
            msa_code,
            msa_name,
            msa_centroid,
            st_geohash(msa_centroid, 6) as msa_centroid_geohash,
            msa_elevation,
        
        from aggregated

    )

select * from final