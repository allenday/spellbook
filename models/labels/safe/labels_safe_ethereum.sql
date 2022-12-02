{{ config(alias='safe_ethereum') }}

SELECT
    address
    , 'safe' AS category
    , 'soispoke' AS contributor
    , 'query' AS source
    , array('ethereum') AS blockchain
    , 'Safe' || ' version ' || creation_version AS name
    , timestamp('2022-09-01') AS created_at
    , now() AS updated_at
FROM {{ ref('safe_ethereum_safes') }}
