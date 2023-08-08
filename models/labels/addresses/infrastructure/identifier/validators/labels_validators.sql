{{config(alias='validators')}}

SELECT * FROM  {{ ref('labels_validators_ethereum') }}
UNION ALL
SELECT * FROM  {{ ref('labels_validators_bnb') }}
UNION ALL
SELECT * FROM  {{ ref('labels_validators_solana') }}