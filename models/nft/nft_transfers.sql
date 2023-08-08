{{ config(
        alias ='transfers',
        partition_by = {"field": "block_date"},
        materialized = 'view',
                        unique_key = ['blockchain', 'unique_transfer_id']
)
}}

{% set nft_models = [
 ref('nft_ethereum_transfers')
,ref('nft_bnb_transfers')
,ref('nft_avalanche_c_transfers')
,ref('nft_gnosis_transfers')
,ref('nft_optimism_transfers')
,ref('nft_arbitrum_transfers')
,ref('nft_polygon_transfers')
,ref('nft_fantom_transfers')
,ref('nft_goerli_transfers')
] %}

SELECT *
FROM (
    {% for nft_model in nft_models %}
    SELECT
          blockchain
        , block_time
        , block_date
        , block_number
        , token_standard
        , transfer_type
        , evt_index
        , contract_address
        , token_id
        , amount
        , `from`
        , `to`
        , executed_by
        , tx_hash
        , unique_transfer_id
    FROM {{ nft_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)