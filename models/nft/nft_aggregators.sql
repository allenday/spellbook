{{ config(
        alias ='aggregators',
        post_hook='{{ expose_spells(\'["avalanche_c","bnb","ethereum","polygon"]\',
                                    "sector",
                                    "nft",
                                    \'["soispoke","hildobby"]\') }}')
}}

SELECT 'avalanche_c' as blockchain, * FROM  {{ ref('nft_avalanche_c_aggregators') }}
UNION DISTINCT
SELECT 'bnb' as blockchain, * FROM  {{ ref('nft_bnb_aggregators') }}
UNION DISTINCT
SELECT 'ethereum' as blockchain, * FROM  {{ ref('nft_ethereum_aggregators') }}
UNION DISTINCT
SELECT 'polygon' as blockchain, * FROM  {{ ref('nft_polygon_aggregators') }}
