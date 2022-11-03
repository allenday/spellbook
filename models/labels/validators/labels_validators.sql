{{config(alias='validators',
        post_hook='{{ expose_spells(\'["ethereum","bnb","solana"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')}}

SELECT * FROM  {{ ref('labels_validators_ethereum') }}
UNION DISTINCT
SELECT * FROM  {{ ref('labels_validators_bnb') }}
UNION DISTINCT
SELECT * FROM  {{ ref('labels_validators_solana') }}
