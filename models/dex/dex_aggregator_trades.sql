
{{ config(
        schema ='dex_aggregator',
        alias ='trades',
        partition_by = {"field": "block_date"},
        materialized = 'view',
                        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
        )
}}

/********************************************************
spells with issues, to be excluded in short term:
-- ,ref('odos_trades') contains duplicates
********************************************************/

{% set dex_aggregator_models = [
 ref('oneinch_ethereum_trades')
 ,ref('bebop_trades')
 ,ref('zeroex_trades')
 ,ref('cow_protocol_trades')
] %}

SELECT *
FROM (
    {% for aggregator_model in dex_aggregator_models %}
    SELECT
          blockchain
         , project
         , version
         , block_date
         , block_time
         , token_bought_symbol
         , token_sold_symbol
         , token_pair
         , token_bought_amount
         , token_sold_amount
         , token_bought_amount_raw
         , token_sold_amount_raw
         , amount_usd
         , token_bought_address
         , token_sold_address
         , taker
         , maker
         , project_contract_address
         , tx_hash
         , tx_from
         , tx_to
         , trace_address --ensure field is explicitly cast as array<bigint> in base models
         , evt_index
    FROM {{ aggregator_model }}
    {% if is_incremental() %}
    WHERE block_date >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)