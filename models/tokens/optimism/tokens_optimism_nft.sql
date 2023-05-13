{{ config(
        alias ='nft'
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "tokens",
                                \'["0xRob"]\') }}'
        )
}}

SELECT
    c.contract_address,
    coalesce(t.name, b.name) AS name,
    coalesce(t.symbol, b.symbol) AS symbol,
    c.standard
FROM {{ ref('tokens_optimism_nft_standards') }} AS c
LEFT JOIN {{ ref('tokens_optimism_nft_curated') }} AS t
    ON c.contract_address = t.contract_address
LEFT JOIN {{ ref('tokens_optimism_nft_bridged_mapping') }} AS b
    ON c.contract_address = b.contract_address
