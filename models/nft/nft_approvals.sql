{{ config(
        alias ='approvals',
        partition_by = {"field": "block_date"},
        materialized = 'view',
                        unique_key = ['blockchain', 'block_number','tx_hash','evt_index'])
}}

{% set nft_models = [
 ref('nft_ethereum_approvals')
] %}

SELECT *
FROM (
    {% for nft_model in nft_models %}
    SELECT
        blockchain
        , block_time
        , block_date
        , block_number
        , address
        , token_standard
        , approval_for_all
        , contract_address
        , token_id
        , approved
        , tx_hash
        --, tx_from
        --, tx_to
        , evt_index
    FROM {{ nft_model }}
    {% if not loop.last %}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
    UNION ALL
    {% endif %}
    {% endfor %}
)