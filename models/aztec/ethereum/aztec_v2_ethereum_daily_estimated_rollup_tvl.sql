{{ config(
    schema = 'aztec_v2_ethereum',
    alias = 'daily_estimated_rollup_tvl',
    partition_by = ['date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['symbol', 'token_address', 'date'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "aztec_v2",
                                \'["Henrystats"]\') }}'
    )
}}

with

rollup_balance_changes AS (
  SELECT CAST(t.evt_block_time AS date) AS date
    , t.symbol
    , t.contract_address AS token_address
    , sum(case WHEN t.from_type = 'Rollup' THEN -1 * value_norm WHEN t.to_type = 'Rollup' THEN value_norm ELSE 0 END) AS net_value_norm
  FROM {{ref('aztec_v2_ethereum_rollupbridge_transfers')}} t
  where t.from_type = 'Rollup' or t.to_type = 'Rollup'
  GROUP BY 1,2,3
)

, token_balances AS (
  SELECT date
    , symbol
    , token_address
    , sum(net_value_norm) over (partition BY symbol,token_address order BY date ASC rows between unbounded preceding AND current row) AS balance
    , lead(date, 1) over (partition BY token_address order BY date) AS next_date
  FROM rollup_balance_changes
)

, day_series AS (
  SELECT explode(sequence(CAST('2022-06-06' AS date), CAST(NOW() AS date), interval '1 Day')) as date
)

, token_balances_filled AS (
  SELECT d.date
    , b.symbol
    , b.token_address
    , b.balance
  FROM day_series d
  inner join token_balances b
        ON d.date >= b.date
        AND d.date < coalesce(b.next_date, CAST(NOW() AS date) + 1) -- if it's missing that means it's the last entry in the series
)

, token_addresses AS (
    SELECT
        DISTINCT(token_address) AS token_address FROM rollup_balance_changes
)

, token_prices_token AS (
    SELECT
        date_trunc('day', p.minute) AS day,
        p.contract_address AS token_address,
        p.symbol,
        AVG(p.price) AS price
    FROM
    token_addresses ta
    INNER JOIN
        {{ source('prices', 'usd') }} p
        ON ta.token_address = p.contract_address
        AND p.blockchain = 'ethereum'
        {% if NOT is_incremental() %}
        AND p.minute >= '{{first_transfer_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND p.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    GROUP BY 1, 2, 3
)

, token_prices_eth AS (
    SELECT
        date_trunc('day', p.minute) AS day,
        AVG(p.price) AS price

    FROM
        {{ source('prices', 'usd') }} p
        {% if NOT is_incremental() %}
        WHERE p.minute >= '{{first_transfer_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE p.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND p.blockchain = 'ethereum'
        AND p.symbol = 'WETH'
    GROUP BY 1
)

, token_prices AS (
    SELECT
        tt.day,
        tt.token_address,
        tt.symbol,
        tt.price AS price_usd,
        tt.price / te.price AS price_eth,
        te.price AS eth_price
    FROM
    token_prices_token tt
    INNER JOIN
    token_prices_eth te
        ON tt.day = te.day
)
, token_tvls AS (
  SELECT b.date
    , b.symbol
    , b.token_address
    , b.balance
    , b.balance * COALESCE(p.price_usd, bb.eth_price) AS tvl_usd
    , b.balance * COALESCE(p.price_eth, 1) AS tvl_eth
  FROM token_balances_filled b
  inner join token_prices p ON b.date = p.day AND b.token_address = p.token_address
  LEFT JOIN token_prices bb ON b.date = bb.day AND b.token_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' -- using this to get price for missing ETH token

)
SELECT * FROM token_tvls
;