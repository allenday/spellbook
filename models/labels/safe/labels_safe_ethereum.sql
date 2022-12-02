{{config(alias='safe_ethereum')}}

SELECT
    array('ethereum') AS blockchain
    , address
    , 'Safe'  || ' version ' || creation_version AS name
    , 'safe' AS category
    , 'soispoke' AS contributor
    , 'query' AS source
    , timestamp('2022-09-01') AS created_at
    , now() AS updated_at
FROM {{ ref('safe_ethereum_safes') }}