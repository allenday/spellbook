{{ config(
        alias ='nft'
        , materialized = 'view'
        
        )
}}

SELECT
    c.contract_address
  , t.name
  , t.symbol
  , c.standard
  FROM {{ ref('tokens_avalanche_c_nft_standards')}} c
LEFT JOIN  {{ref('tokens_avalanche_c_nft_curated')}} t
ON c.contract_address = t.contract_address