{{ config(alias='miners',
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')
}}

SELECT DISTINCT
    array('ethereum') AS blockchain
    , miner
    , 'Ethereum Miner' AS name
    , 'contracts' AS category
    , 'soispoke' AS contributor
    , 'query' AS source
    , date('2022-09-28') AS created_at
    , now() AS modified_at
FROM {{ source('ethereum', 'blocks') }}
UNION
SELECT DISTINCT
    array('gnosis') AS blockchain
    , miner
    , 'Gnosis Miner' AS name
    , 'contracts' AS category
    , 'soispoke' AS contributor
    , 'query' AS source
    , date('2022-09-28') AS created_at
    , now() AS modified_at
FROM {{ source('gnosis', 'blocks') }}
UNION
SELECT DISTINCT
    array('avalanche_c') AS blockchain
    , miner
    , 'Avalanche Miner' AS name
    , 'contracts' AS category
    , 'soispoke' AS contributor
    , 'query' AS source
    , date('2022-09-28') AS created_at
    , now() AS modified_at
FROM {{ source('avalanche_c', 'blocks') }}
UNION
SELECT DISTINCT
    array('arbitrum') AS blockchain
    , miner
    , 'Arbitrum Miner' AS name
    , 'contracts' AS category
    , 'soispoke' AS contributor
    , 'query' AS source
    , date('2022-09-28') AS created_at
    , now() AS modified_at
FROM {{ source('arbitrum', 'blocks') }}
UNION
SELECT DISTINCT
    array('bnb') AS blockchain
    , miner
    , 'BNB Chain Miner' AS name
    , 'contracts' AS category
    , 'soispoke' AS contributor
    , 'query' AS source
    , date('2022-09-28') AS created_at
    , now() AS modified_at
FROM {{ source('bnb', 'blocks') }}
UNION
SELECT DISTINCT
    array('optimism') AS blockchain
    , miner
    , 'Optimism Miner' AS name
    , 'contracts' AS category
    , 'soispoke' AS contributor
    , 'query' AS source
    , date('2022-09-28') AS created_at
    , now() AS modified_at
FROM {{ source('optimism', 'blocks') }}
