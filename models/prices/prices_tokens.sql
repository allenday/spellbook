{{ config(
        schema='prices',
        alias ='tokens',
        materialized = 'view',
                tags=['static']
        )
}}

{% set prices_models = [
ref('prices_native_tokens')
,ref('prices_ethereum_tokens')
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