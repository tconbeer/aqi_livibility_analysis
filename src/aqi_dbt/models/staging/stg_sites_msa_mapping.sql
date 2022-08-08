with
    seed as (select * from {{ ref("seed_sites_msa_mapping") }}),
    final as (select site_id, msa_code, msa_name, from seed)
select *
from final
