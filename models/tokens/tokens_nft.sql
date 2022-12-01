{{ config( alias='nft',
        post_hook='{{ expose_spells(\'["avalanche_c","bnb","ethereum","optimism", "gnosis"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xManny","hildobby","soispoke","dot2dotseurat"]\') }}')}}

SELECT
'avalanche_c' AS blockchain,
contract_address,
name,
symbol,
standard,
category
FROM  {{ ref('tokens_avalanche_c_nft') }}
            UNION
SELECT
'ethereum' AS blockchain,
contract_address,
name,
symbol,
standard,
category
FROM  {{ ref('tokens_ethereum_nft') }}
            UNION
SELECT
'gnosis' AS blockchain,
contract_address,
name,
symbol,
standard,
CAST(NULL AS VARCHAR(5)) AS category
FROM  {{ ref('tokens_gnosis_nft') }}
            UNION
SELECT
'optimism' AS blockchain,
contract_address,
name,
CAST(NULL AS VARCHAR(5)) AS symbol,
CAST(NULL AS VARCHAR(5)) AS standard,
CAST(NULL AS VARCHAR(5)) AS category
FROM  {{ ref('tokens_optimism_nft') }}
            UNION
SELECT
'optimism' AS blockchain,
contract_address,
name,
symbol,
standard,
category
FROM  {{ ref('tokens_optimism_nft_bridged_mapping') }}
            UNION
SELECT
'bnb' AS blockchain,
contract_address,
name,
CAST(NULL AS VARCHAR(5)) AS symbol,
standard,
CAST(NULL AS VARCHAR(5)) AS category
FROM  {{ ref('tokens_bnb_nft') }}
