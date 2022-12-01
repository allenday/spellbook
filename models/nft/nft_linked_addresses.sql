{{ config(
    alias = 'linked_addresses',
    partition_by = ['blockchain'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'linked_address_id'],
    post_hook='{{ expose_spells(\'["ethereum","solana"]\',
                                "sector",
                                "nft",
                                \'["springzh"]\') }}'
    )
}}

with nft_trade_address AS (
    SELECT distinct blockchain, buyer AS address_a, seller as address_b
    from {{ ref('nft_trades') }}
    where buyer is NOT null
        and seller is NOT null
        and blockchain is NOT null
    {% if is_incremental() %}
    and block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    union all

    SELECT distinct blockchain, seller AS address_a, buyer as address_b
    from {{ ref('nft_trades') }}
    where buyer is NOT null
        and seller is NOT null
        and blockchain is NOT null
    {% if is_incremental() %}
    and block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

linked_address_nft_trade AS (
    SELECT blockchain,
        address_a,
        address_b,
        count(*) AS cnt
    from nft_trade_address
    group by 1, 2, 3
    having count(*) > 1
),

linked_address_sorted AS (
    -- Normalize linked addresses to master address
    SELECT blockchain,
        (case when address_a > address_b then address_b else address_a end) AS master_address,
        address_a AS alternative_address
    from linked_address_nft_trade
    union
    SELECT blockchain,
        (case when address_a > address_b then address_b else address_a end) AS master_address,
        address_b AS alternative_address
    from linked_address_nft_trade
),

linked_address_sorted_row_num AS (
    SELECT blockchain, master_address, alternative_address,
        master_address || '-' || alternative_address AS linked_address_id,
        row_number() over (partition by blockchain, alternative_address order by master_address) AS row_num
    from linked_address_sorted
)

SELECT blockchain,
    master_address,
    alternative_address,
    linked_address_id
from linked_address_sorted_row_num
where row_num = 1