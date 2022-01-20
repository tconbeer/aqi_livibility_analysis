{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            'field': 'observed_date', 
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by = ['site_id', 'parameter_name', 'observed_date'],
        unique_key = 'id',
    )
}}

with
    source_table as (select * from {{ source('aqi_raw_data', 'aqi_sites') }}),
    renamed as (
        
        select
            {{ dbt_utils.surrogate_key(['observed_date', 'site_id', 'parameter_name']) }}
            as id,
            
            observed_date,
            
            parameter_name,
            
            site_id,
            site_name,
            
            data_source_id,
            data_source_agency,
            
            latitude,
            longitude,
            safe_cast(elevation as float64) as elevation,
            
            epa_region,
            coalesce(
                country_code, case when data_source_id = 'MOE' then 'JP' end
            ) as country_code,
            msa_code,
            trim(msa_name) as msa_name,
            state_code,
            state_name,
            county_code,
            county_name,
            
        from source_table
        where
            1 = 1
            {% if is_incremental() -%}
            and observed_date > (select max(observed_date) from {{ this }})
            {%- endif %}
            
        -- there are about 5k duplicates in the raw data
        qualify
            row_number() over (partition by observed_date, site_id, parameter_name) = 1
    )
    
select *
from renamed
