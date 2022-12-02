{{config(alias='airdrop_1_receivers_optimism')}}

SELECT
    address
    , '$OP Airdrop 1 Receiver' AS name
    , 'airdrop' AS category
    , 'soispoke' AS contributor
    , 'query' AS source
    , array('optimism') AS blockchain
    , timestamp('2022-09-29') AS created_at
    , now() AS updated_at
FROM {{ ref('airdrop_optimism_addresses_1') }}
