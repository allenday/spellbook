{{config(alias='eth_stakers')}}

WITH identified_stakers AS (
    SELECT 'ethereum' AS blockchain
    , address
    , entity AS name
    , 'infrastructure' AS category
    , 'hildobby' AS contributor
    , 'query' AS source
    , timestamp('2023-01-18') AS created_at
    , CURRENT_TIMESTAMP() AS updated_at
    , 'eth_stakers' AS model_name
    , 'identifier' as label_type
    FROM {{ ref('staking_ethereum_entities')}}
    )

, unidentified_stakers AS (
    SELECT 'ethereum' AS blockchain
    , et.from AS address
    , 'Unidentified ETH staker' AS name
    , 'infrastructure' AS category
    , 'hildobby' AS contributor
    , 'query' AS source
    , timestamp('2023-01-18') AS created_at
    , CURRENT_TIMESTAMP() AS updated_at
    , 'eth_stakers' AS model_name
    , 'identifier' as label_type
    FROM {{ source('ethereum', 'traces') }} et
    LEFT JOIN identified_stakers is ON et.from = is.address
    WHERE et.to IS NULL AND et.to = '0x00000000219ab540356cbb839cbe05303d7705fa'
    AND et.success
    AND CAST(et.value AS FLOAT64) > 0
    GROUP BY et.from
    )

SELECT * FROM identified_stakers
UNION ALL
SELECT * FROM unidentified_stakers