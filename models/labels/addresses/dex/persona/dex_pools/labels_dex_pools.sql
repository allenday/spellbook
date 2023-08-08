{{
    config(
        alias='dex_pools'
    )
}}

SELECT blockchain
, pool AS address
, "DEX Pool" AS name
, "dex" AS category
, "hildobby" AS contributor
, "query" AS source
, timestamp('2023-03-18') as created_at
, CURRENT_TIMESTAMP() AS updated_at
, "dex_pools" AS model_name
, "persona" AS label_type
FROM {{ ref('dex_pools') }}