{{ config(
        alias ='native_trades',
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon","bnb"]\',
                                "project",
                                "zeroex",
                                \'["rantum","bakabhai993"]\') }}'
        )
}}

-- sample dune query for this model


{% set zeroex_models = [  
ref('zeroex_native_fills')
] %}


SELECT *
FROM (
    {% for model in zeroex_models %}
        SELECT
            blockchain AS blockchain,
            '0x API' AS project,
            version,
            block_date AS block_date,
            block_time AS block_time,
            maker_symbol AS token_bought_symbol,
            taker_symbol AS token_sold_symbol,
            token_pair AS token_pair,
            maker_token_amount AS token_bought_amount,
            taker_token_amount AS token_sold_amount,
            maker_token_amount_raw AS token_bought_amount_raw,
            taker_token_amount_raw AS token_sold_amount_raw,
            volume_usd AS amount_usd,
            maker_token AS token_bought_address,
            taker_token AS token_sold_address,
            taker AS taker,
            maker AS maker,
            contract_address AS project_contract_address,
            tx_hash AS tx_hash,
            tx_from AS tx_from,
            tx_to AS tx_to,
            trace_address,
            evt_index AS evt_index

        FROM {{ model }}
        {% if not loop.last %}
     
    UNION ALL
   
    {% endif %}
    {% endfor %}
);
