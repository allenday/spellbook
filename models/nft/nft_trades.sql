{{ config(
        alias ='trades',
        partition_by = {"field": "block_date"},
        materialized = 'view',
                        unique_key = ['unique_trade_id', 'blockchain'])
}}


{% set nft_models = [
 ref('archipelago_ethereum_trades')
,ref('blur_ethereum_trades')
,ref('cryptopunks_ethereum_trades')
,ref('element_trades')
,ref('foundation_ethereum_trades')
,ref('liquidifty_trades')
,ref('looksrare_ethereum_trades')
,ref('magiceden_trades')
,ref('opensea_trades')
,ref('sudoswap_ethereum_trades')
,ref('superrare_ethereum_trades')
,ref('trove_trades')
,ref('x2y2_ethereum_trades')
,ref('zora_ethereum_trades')
,ref('tofu_trades')
,ref('collectionswap_ethereum_trades')
] %}

SELECT *
FROM (
    {% for nft_model in nft_models %}
    SELECT
        blockchain,
        project,
        version,
        TIMESTAMP_TRUNC(block_time, day)  as block_date,
        block_time,
        token_id,
        collection,
        amount_usd,
        token_standard,
        trade_type,
        number_of_items,
        trade_category,
        evt_type,
        seller,
        buyer,
        amount_original,
        amount_raw,
        currency_symbol,
        currency_contract,
        nft_contract_address,
        project_contract_address,
        aggregator_name,
        aggregator_address,
        tx_hash,
        block_number,
        tx_from,
        tx_to,
        unique_trade_id
    FROM {{ nft_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}

)