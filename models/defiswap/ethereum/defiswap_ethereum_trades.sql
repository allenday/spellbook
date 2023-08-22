{{ config(
    alias ='trades',
    partition_by = {"field": "block_date"},
    materialized = 'view',
            unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2020-09-09' %}

WITH dexs AS (
    -- defiswap
    SELECT
        t.evt_block_time AS block_time
        , t.`to` AS taker
        , t.contract_address AS maker
        , CASE WHEN amount0Out = 0 THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw
        , CASE WHEN amount0In = 0 OR amount1Out = 0 THEN amount1In ELSE amount0In END AS token_sold_amount_raw
        , NULL AS amount_usd
        , CASE WHEN amount0Out = 0 THEN f.token1 ELSE f.token0 END AS token_bought_address
        , CASE WHEN amount0In = 0 OR amount1Out = 0 THEN f.token1 ELSE f.token0 END AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , '' AS trace_address
        , t.evt_index AS evt_index
    FROM
        {{ source('defiswap_ethereum', 'CroDefiSwapPair_evt_Swap') }} AS t
    INNER JOIN {{ source('crodefi_ethereum', 'CroDefiSwapFactory_evt_PairCreated') }} AS f ON f.pair = t.contract_address
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
)

SELECT
    'ethereum' AS blockchain
    , 'defiswap' AS project
    , '1' AS version
    , SAFE_CAST(TIMESTAMP_TRUNC(dexs.block_time, DAY) AS date) AS block_date
    , dexs.block_time
    , erc20a.symbol AS token_bought_symbol
    , erc20b.symbol AS token_sold_symbol
    , CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, '-', erc20a.symbol)
        ELSE concat(erc20a.symbol, '-', erc20b.symbol)
    END AS token_pair
    , dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    , dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount
    , CAST(dexs.token_bought_amount_raw AS bignumeric) AS token_bought_amount_raw
    , CAST(dexs.token_sold_amount_raw AS bignumeric) AS token_sold_amount_raw
    , coalesce(
        dexs.amount_usd,
        dexs.token_bought_amount_raw / power(10, erc20a.decimals) * pa.price,
        dexs.token_sold_amount_raw / power(10, erc20b.decimals) * pb.price
    ) AS amount_usd
    , dexs.token_bought_address
    , dexs.token_sold_address
    , coalesce(dexs.taker, tx.from) AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , tx.`from` AS tx_from
    , tx.`to` AS tx_to
    , dexs.trace_address
    , dexs.evt_index
FROM dexs
INNER JOIN {{ source('ethereum', 'transactions') }} AS tx
    ON dexs.tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} AS erc20a
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = 'ethereum'
LEFT JOIN {{ ref('tokens_erc20') }} AS erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'ethereum'
LEFT JOIN {{ source('prices', 'usd') }} AS pa
    ON pa.minute = TIMESTAMP_TRUNC(dexs.block_time, MINUTE)
    AND pa.contract_address = dexs.token_bought_address
    AND pa.blockchain = 'ethereum'
    {% if not is_incremental() %}
    AND pa.minute >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    AND pa.minute >= date_trunc('day', CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} AS pb
    ON pb.minute = TIMESTAMP_TRUNC(dexs.block_time, MINUTE)
    AND pb.contract_address = dexs.token_sold_address
    AND pb.blockchain = 'ethereum'
    {% if not is_incremental() %}
    AND pb.minute >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    AND pb.minute >= date_trunc('day', CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}