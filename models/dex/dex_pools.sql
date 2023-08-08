{{ config(
        alias ='pools',
        materialized = 'view',
                        unique_key = ['blockchain', 'pool']
        )
}}


{% set dex_pool_models = [
 ref('uniswap_pools')
] %}


SELECT *
FROM (
    {% for dex_pool_model in dex_pool_models %}
    SELECT
        blockchain
        , project
        , version
        , pool
        , fee
        , token0
        , token1
        , creation_block_time
        , creation_block_number
        , contract_address
    FROM {{ dex_pool_model }}
    {% if not loop.last %}
    {% if is_incremental() %}
    WHERE creation_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
    UNION ALL
    {% endif %}
    {% endfor %}
)