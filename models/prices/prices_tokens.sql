{{ config(
        schema='prices',
        alias ='tokens',
        materialized = 'view',
                tags=['static']
        )
}}

{% set prices_models = [
ref('prices_native_tokens')
,ref('prices_arbitrum_tokens')
,ref('prices_avalanche_c_tokens')
,ref('prices_bnb_tokens')
,ref('prices_ethereum_tokens')
,ref('prices_fantom_tokens')
,ref('prices_gnosis_tokens')
,ref('prices_optimism_tokens')
,ref('prices_polygon_tokens')
,ref('prices_solana_tokens')
] %}


SELECT *
FROM
(
    {% for model in prices_models %}
    SELECT
        token_id
        , blockchain
        , symbol
        , contract_address
        , decimals
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)