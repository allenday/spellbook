{{ config(
    alias = 'linked_addresses',
    partition_by = {"field": "blockchain"},
    materialized = 'view',
            unique_key = ['blockchain', 'master_address','alternative_address' ]
    )
}}


select distinct blockchain,
    case when buyer <= seller then buyer else seller end as master_address,
    case when buyer <= seller then seller else buyer end as alternative_address,
    max(block_time) as last_trade
from {{ ref('nft_trades') }}
where buyer is not null
    and seller is not null
    and blockchain is not null
{% if is_incremental() %}
and block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
{% endif %}
GROUP BY 1,2,3