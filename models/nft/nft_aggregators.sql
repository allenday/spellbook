{{ config(
        alias ='aggregators',
        post_hook='{{ expose_spells(\'["avalanche_c","bnb","ethereum","polygon", "optimism"]\',
                                    "sector",
                                    "nft",
                                    \'["soispoke","hildobby", "chuxin"]\') }}')
}}

SELECT
    'avalanche_c' AS blockchain,
    *
FROM {{ ref('nft_avalanche_c_aggregators') }}
UNION ALL
SELECT
    'bnb' AS blockchain,
    *
FROM {{ ref('nft_bnb_aggregators') }}
UNION ALL
SELECT
    'ethereum' AS blockchain,
    *
FROM {{ ref('nft_ethereum_aggregators') }}
UNION ALL
SELECT
    'polygon' AS blockchain,
    *
FROM {{ ref('nft_polygon_aggregators') }}
UNION ALL
SELECT
    'optimism' AS blockchain,
    *
FROM {{ ref('nft_optimism_aggregators') }}
