{{ config(
        schema='oneinch_v1_ethereum',
        alias='trades',
        partition_by = ['block_date'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2019-06-03' %} --for testing, use small subset of data
{% set generic_null_address = '0x0000000000000000000000000000000000000000' %} --according to etherscan label
{% set burn_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %} --according to etherscan label
{% set blockchain = 'ethereum' %}
{% set blockchain_symbol = 'ETH' %}

{% set final_columns = [
    'block_number'
    ,'taker'
    ,'from_token'
    ,'to_token'
    ,'from_amount'
    ,'to_amount'
    ,'tx_hash'
    ,'block_time'
    ,'trace_address'
    ,'evt_index'
    ,'contract_address'
] %}

WITH oneinch_calls AS (
    SELECT
        call_block_number AS block_number,
        CAST(NULL AS string) AS taker,
        fromtoken AS from_token,
        totoken AS to_token,
        tokensamount AS from_amount,
        mintokensamount AS to_amount,
        call_tx_hash AS tx_hash,
        call_block_time AS block_time,
        call_trace_address AS trace_address,
        CAST(-1 AS integer) AS evt_index,
        contract_address
    FROM
        {{ source('oneinch_ethereum', 'exchange_v1_call_aggregate') }}
    WHERE
        call_success
        {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval "1 week")
        {% else %}
        AND call_block_time >= '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        call_block_number AS block_number,
        CAST(NULL AS string) AS taker,
        fromtoken AS from_token,
        totoken AS to_token,
        tokensamount AS from_amount,
        mintokensamount AS to_amount,
        call_tx_hash AS tx_hash,
        call_block_time AS block_time,
        call_trace_address AS trace_address,
        CAST(-1 AS integer) AS evt_index,
        contract_address
    FROM
        {{ source('oneinch_ethereum', 'exchange_v2_call_aggregate') }}
    WHERE
        call_success
        {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval "1 week")
        {% else %}
        AND call_block_time >= '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        call_block_number AS block_number,
        CAST(NULL AS string) AS taker,
        fromtoken AS from_token,
        totoken AS to_token,
        tokensamount AS from_amount,
        mintokensamount AS to_amount,
        call_tx_hash AS tx_hash,
        call_block_time AS block_time,
        call_trace_address AS trace_address,
        CAST(-1 AS integer) AS evt_index,
        contract_address
    FROM
        {{ source('oneinch_ethereum', 'exchange_v3_call_aggregate') }}
    WHERE
        call_success
        {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval "1 week")
        {% else %}
        AND call_block_time >= '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        call_block_number AS block_number,
        CAST(NULL AS string) AS taker,
        fromtoken AS from_token,
        totoken AS to_token,
        tokensamount AS from_amount,
        mintokensamount AS to_amount,
        call_tx_hash AS tx_hash,
        call_block_time AS block_time,
        call_trace_address AS trace_address,
        CAST(-1 AS integer) AS evt_index,
        contract_address
    FROM
        {{ source('oneinch_ethereum', 'exchange_v4_call_aggregate') }}
    WHERE
        call_success
        {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval "1 week")
        {% else %}
        AND call_block_time >= '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        call_block_number AS block_number,
        CAST(NULL AS string) AS taker,
        fromtoken AS from_token,
        totoken AS to_token,
        tokensamount AS from_amount,
        mintokensamount AS to_amount,
        call_tx_hash AS tx_hash,
        call_block_time AS block_time,
        call_trace_address AS trace_address,
        CAST(-1 AS integer) AS evt_index,
        contract_address
    FROM
        {{ source('oneinch_ethereum', 'exchange_v5_call_aggregate') }}
    WHERE
        call_success
        {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval "1 week")
        {% else %}
        AND call_block_time >= '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        call_block_number AS block_number,
        CAST(NULL AS string) AS taker,
        fromtoken AS from_token,
        totoken AS to_token,
        tokensamount AS from_amount,
        mintokensamount AS to_amount,
        call_tx_hash AS tx_hash,
        call_block_time AS block_time,
        call_trace_address AS trace_address,
        CAST(-1 AS integer) AS evt_index,
        contract_address
    FROM
        {{ source('oneinch_ethereum', 'exchange_v6_call_aggregate') }}
    WHERE
        call_success
        {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval "1 week")
        {% else %}
        AND call_block_time >= '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        call_block_number AS block_number,
        CAST(NULL AS string) AS taker,
        fromtoken AS from_token,
        totoken AS to_token,
        fromtokenamount AS from_amount,
        minreturnamount AS to_amount,
        call_tx_hash AS tx_hash,
        call_block_time AS block_time,
        call_trace_address AS trace_address,
        CAST(-1 AS integer) AS evt_index,
        contract_address
    FROM
        {{ source('oneinch_ethereum', 'exchange_v7_call_swap') }}
    WHERE
        call_success
        {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval "1 week")
        {% else %}
        AND call_block_time >= '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        call_block_number AS block_number,
        CAST(NULL AS string) AS taker,
        fromtoken AS from_token,
        totoken AS to_token,
        fromtokenamount AS from_amount,
        minreturnamount AS to_amount,
        call_tx_hash AS tx_hash,
        call_block_time AS block_time,
        call_trace_address AS trace_address,
        CAST(-1 AS integer) AS evt_index,
        contract_address
    FROM
        {{ source('oneinch_ethereum', 'OneInchExchange_call_swap') }}
    WHERE
        call_success
        {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval "1 week")
        {% else %}
        AND call_block_time >= '{{ project_start_date }}'
        {% endif %}
),

oneinch AS (
    SELECT
        block_number,
        block_time,
        "1inch" AS project,
        "1" AS version,
        taker,
        CAST(NULL AS string) AS maker,
        to_amount AS token_bought_amount_raw,
        from_amount AS token_sold_amount_raw,
        CAST(NULL AS double) AS amount_usd,
        CASE
            WHEN to_token = '{{ generic_null_address }}'
                THEN '{{ burn_address }}'
            ELSE to_token
        END AS token_bought_address,
        CASE
            WHEN from_token = '{{ generic_null_address }}'
                THEN '{{ burn_address }}'
            ELSE from_token
        END AS token_sold_address,
        contract_address AS project_contract_address,
        tx_hash,
        trace_address,
        evt_index
    FROM
        (
            SELECT
                {% for column in final_columns %}
                    {% if not loop.first %},{% endif %} {{ column }}
                {% endfor %}
            FROM
                oneinch_calls
        )
)

SELECT
    '{{ blockchain }}' AS blockchain,
    src.project,
    src.version,
    date_trunc("day", src.block_time) AS block_date,
    src.block_time,
    src.block_number,
    token_bought.symbol AS token_bought_symbol,
    token_sold.symbol AS token_sold_symbol,
    CASE
        WHEN lower(token_bought.symbol) > lower(token_sold.symbol) THEN concat(token_sold.symbol, "-", token_bought.symbol)
        ELSE concat(token_bought.symbol, "-", token_sold.symbol)
    END AS token_pair,
    src.token_bought_amount_raw / power(10, token_bought.decimals) AS token_bought_amount,
    src.token_sold_amount_raw / power(10, token_sold.decimals) AS token_sold_amount,
    CAST(src.token_bought_amount_raw AS decimal(38, 0)) AS token_bought_amount_raw,
    CAST(src.token_sold_amount_raw AS decimal(38, 0)) AS token_sold_amount_raw,
    coalesce(
        src.amount_usd,
        (
            src.token_bought_amount_raw / power(
                10,
                CASE
                    WHEN token_bought_address = '{{ burn_address }}'
                        THEN 18
                    ELSE prices_bought.decimals
                END
            )
        )
        *
        (
            CASE
                WHEN token_bought_address = '{{ burn_address }}'
                    THEN prices_eth.price
                ELSE prices_bought.price
            END
        ),
        (
            src.token_sold_amount_raw / power(
                10,
                CASE
                    WHEN token_sold_address = '{{ burn_address }}'
                        THEN 18
                    ELSE prices_sold.decimals
                END
            )
        )
        *
        (
            CASE
                WHEN token_sold_address = '{{ burn_address }}'
                    THEN prices_eth.price
                ELSE prices_sold.price
            END
        )
    ) AS amount_usd,
    src.token_bought_address,
    src.token_sold_address,
    coalesce(src.taker, tx.from) AS taker,
    src.maker,
    src.project_contract_address,
    src.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    CAST(src.trace_address AS ARRAY<long>) AS trace_address,
    src.evt_index
FROM oneinch AS src
INNER JOIN {{ source('ethereum', 'transactions') }} AS tx
    ON
        src.tx_hash = tx.hash
        AND src.block_number = tx.block_number
        {% if is_incremental() %}
            AND tx.block_time >= date_trunc("day", now() - interval "1 week")
        {% else %}
    AND tx.block_time >= '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} AS token_bought
    ON
        token_bought.contract_address = src.token_bought_address
        AND token_bought.blockchain = '{{ blockchain }}'
LEFT JOIN {{ ref('tokens_erc20') }} AS token_sold
    ON
        token_sold.contract_address = src.token_sold_address
        AND token_sold.blockchain = '{{ blockchain }}'
LEFT JOIN {{ source('prices', 'usd') }} AS prices_bought
    ON
        prices_bought.minute = date_trunc("minute", src.block_time)
        AND prices_bought.contract_address = src.token_bought_address
        AND prices_bought.blockchain = '{{ blockchain }}'
        {% if is_incremental() %}
            AND prices_bought.minute >= date_trunc("day", now() - interval "1 week")
        {% else %}
    AND prices_bought.minute >= '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} AS prices_sold
    ON
        prices_sold.minute = date_trunc("minute", src.block_time)
        AND prices_sold.contract_address = src.token_sold_address
        AND prices_sold.blockchain = '{{ blockchain }}'
        {% if is_incremental() %}
            AND prices_sold.minute >= date_trunc("day", now() - interval "1 week")
        {% else %}
    AND prices_sold.minute >= '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} AS prices_eth
    ON
        prices_eth.minute = date_trunc("minute", src.block_time)
        AND prices_eth.blockchain IS NULL
        AND prices_eth.symbol = '{{ blockchain_symbol }}'
        {% if is_incremental() %}
            AND prices_eth.minute >= date_trunc("day", now() - interval "1 week")
        {% else %}
    AND prices_eth.minute >= '{{ project_start_date }}'
    {% endif %}
;
