{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "observed_date",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["msa_name"],
        unique_key="id",
    )
}}


{% set aqi_livibility_threshold = 90 %}

with
    fct_aqi_hourly_observations as (
        select * from {{ ref("fct_aqi_hourly_observations") }}
    ),
    dim_msa as (select * from {{ ref("dim_msa") }}),

    msa_grouped as (
        select
            site_data.msa_code as msa_code,
            observed_at,
            observed_date,

            avg(observed_aqi) as mean_observed_aqi,
            max(observed_aqi) as max_observed_aqi,
            count(*) as number_of_sites,

            array_agg(site_data) as sites,
            array_concat_agg(observations) as observations,

        from fct_aqi_hourly_observations

        where
            site_data.msa_name is not null
            {% if is_incremental() -%}
                and observed_date >= (select max(observed_date) from {{ this }})
            {%- endif %}

            {{ dbt_utils.group_by(3) }}
    ),

    daily_grouped as (
        select
            msa_code,
            observed_date,

            avg(mean_observed_aqi) as mean_observed_aqi,
            max(max_observed_aqi) as max_observed_aqi,

            array_agg(
                struct(
                    extract(hour from observed_at) as hour_of_day,
                    mean_observed_aqi,
                    max_observed_aqi
                )
                respect nulls
                order by observed_at asc
            ) as hourly_observed_aqis,

            max(number_of_sites) as number_of_sites,
            array_concat_agg(sites) as sites,
            array_concat_agg(observations) as observations,

            count(
                case when mean_observed_aqi > {{ aqi_livibility_threshold }} then 1 end
            ) as hours_mean_above_aqi_threshold,

            count(
                case when max_observed_aqi > {{ aqi_livibility_threshold }} then 1 end
            ) as hours_max_above_aqi_threshold,

        from msa_grouped

        group by 1, 2
    ),

    final as (

        select
            {{ dbt_utils.surrogate_key(["msa_name", "observed_date"]) }} as id,
            daily_grouped.*,
            dim_msa.msa_name,
            dim_msa.msa_centroid,
            dim_msa.msa_centroid_geohash,
            dim_msa.msa_elevation,

        from daily_grouped
        join dim_msa on daily_grouped.msa_code = dim_msa.msa_code

    )

select *
from final
