{{ config(
        alias ='nft'
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "tokens",
                                \'["hildobby"]\') }}'
        )
}}

-- reservoir has multiple collection names for single contracts, we just take the first record
WITH reservoir_names AS (
    SELECT
        contract AS contract_address,
        min_by(name, created_at) AS name
    FROM {{ source('reservoir','collections') }}
    GROUP BY 1
)

SELECT
    c.contract_address,
    coalesce(curated.name, reservoir.name) AS name,
    curated.symbol,
    c.standard
FROM {{ ref('tokens_ethereum_nft_standards') }} AS c
LEFT JOIN {{ ref('tokens_ethereum_nft_curated') }} AS curated
    ON c.contract_address = curated.contract_address
LEFT JOIN reservoir_names AS reservoir
    ON c.contract_address = reservoir.contract_address
