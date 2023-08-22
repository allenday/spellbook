{{ config(
        alias ='trades',
        partition_by = {"field": "block_date"},
        materialized = 'view',
                        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
        )
}}


{% set dex_trade_models = [
 ref('uniswap_trades')
,ref('sushiswap_trades')
,ref('kyberswap_trades')
,ref('fraxswap_trades')
,ref('curvefi_trades')
,ref('airswap_ethereum_trades')
,ref('clipper_trades')
,ref('shibaswap_ethereum_trades')
,ref('swapr_ethereum_trades')
,ref('defiswap_ethereum_trades')
,ref('dfx_ethereum_trades')
,ref('pancakeswap_trades')
,ref('dodo_trades')
,ref('bancor_ethereum_trades')
,ref('hashflow_trades')
,ref('mstable_ethereum_trades')
,ref('apeswap_trades')
,ref('balancer_trades')
,ref('maverick_trades')
,ref('verse_dex_ethereum_trades')
] %}


SELECT *
FROM (
    {% for dex_model in dex_trade_models %}
    SELECT
        blockchain,
        project,
        version,
        block_date,
        block_time,
        token_bought_symbol,
        token_sold_symbol,
        token_pair,
        token_bought_amount,
        token_sold_amount,
        token_bought_amount_raw,
        token_sold_amount_raw,
        amount_usd,
        token_bought_address,
        token_sold_address,
        taker,
        maker,
        project_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        trace_address,
        evt_index
    FROM {{ dex_model }}
    {% if not loop.last %}
    {% if is_incremental() %}
    WHERE block_date >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
    UNION ALL
    {% endif %}
    {% endfor %}
)