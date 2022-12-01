{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "hashflow",
                                \'["justabi", "jeff-dude"]\') }}'
    )
}}

{% set project_start_date = '2021-04-28' %}

with hashflow_trades AS (
    SELECT *
    FROM {{ ref('hashflow_ethereum_raw_trades') }}
    where fill_status is true -- successful trade
    {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '10 days')
    {% endif %}
),

ethereum_transactions AS (
    SELECT *
    FROM {{ source('ethereum', 'transactions') }}
    where block_time >= '{{ project_start_date }}'
    {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '10 days')
    {% endif %}
),

erc20_tokens AS (
    SELECT *
    FROM {{ ref('tokens_erc20') }}
    where blockchain = 'ethereum'
)

SELECT
    'ethereum' AS blockchain,
    'hashflow' AS project,
    '1' AS version,
    block_date,
    hashflow_trades.block_time,
    hashflow_trades.maker_symbol AS token_bought_symbol,
    hashflow_trades.taker_symbol AS token_sold_symbol,
    CASE WHEN lower(hashflow_trades.maker_symbol) > lower(hashflow_trades.taker_symbol)
            THEN concat(hashflow_trades.taker_symbol, '-', hashflow_trades.maker_symbol)
        ELSE concat(hashflow_trades.maker_symbol, '-', hashflow_trades.taker_symbol) END AS token_pair,
    hashflow_trades.maker_token_amount AS token_bought_amount,
    hashflow_trades.taker_token_amount AS token_sold_amount,
    CAST(hashflow_trades.maker_token_amount * power(10, erc20a.decimals) AS DECIMAL(38, 0)) AS token_bought_amount_raw,
    CAST(hashflow_trades.taker_token_amount * power(10, erc20b.decimals) AS DECIMAL(38, 0)) AS token_sold_amount_raw,
    hashflow_trades.amount_usd,
    hashflow_trades.maker_token AS token_bought_address,
    hashflow_trades.taker_token AS token_sold_address,
    hashflow_trades.trader AS taker,
    hashflow_trades.pool AS maker,
    hashflow_trades.router_contract AS project_contract_address,
    hashflow_trades.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    '' AS trace_address,
    CASE WHEN hashflow_trades.composite_index <> -1 THEN hashflow_trades.composite_index END AS evt_index
FROM hashflow_trades
inner join ethereum_transactions tx
    ON hashflow_trades.tx_hash = tx.hash
LEFT JOIN erc20_tokens erc20a
    ON erc20a.contract_address = hashflow_trades.maker_token
LEFT JOIN erc20_tokens erc20b
    ON erc20b.contract_address = hashflow_trades.taker_token
;
