{{ config(
    schema = 'aztec_v2_ethereum',
    alias = 'daily_bridge_activity',
    partition_by = ['date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['bridge_protocol', 'date', 'bridge_address', 'token_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "aztec_v2",
                                \'["Henrystats"]\') }}'
    )
}}

{% set first_transfer_date = '2022-06-06' %} -- first tx date

WITH

daily_transfers AS (
    SELECT
        date_trunc('day', evt_block_time) AS date
        , bridge_protocol
        , bridge_address
        , contract_address AS token_address
        , count(*) AS num_tfers -- number of transfers
        , count(distinct evt_tx_hash) AS num_rollups -- number of rollups
        , sum(case when spec_txn_type in ('Bridge to Protocol','Protocol to Bridge') then value_norm else 0 end ) AS abs_value_norm
        , sum(case when spec_txn_type = 'Bridge to Protocol' then value_norm else 0 end ) AS input_value_norm
        , sum(case when spec_txn_type = 'Protocol to Bridge' then value_norm else 0 end ) AS output_value_norm
    FROM {{ref('aztec_v2_ethereum_rollupbridge_transfers')}}
    where bridge_protocol is NOT NULL -- exclude all txns that don't interact with the bridges
    GROUP BY 1,2,3,4
),

token_addresses AS (
    SELECT
        DISTINCT(token_address) AS token_address FROM daily_transfers
),

token_prices_token AS (
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
),

token_prices_eth AS (
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
),

token_prices AS (
    SELECT
        tt.day,
        tt.token_address,
        tt.symbol,
        tt.price AS price_usd,
        tt.price / te.price AS price_eth,
        te.price AS eth_price -- to be used later
    FROM
    token_prices_token tt
    INNER JOIN
    token_prices_eth te
        ON tt.day = te.day
)

 SELECT dt.date
        , dt.bridge_protocol
        , dt.bridge_address
        , dt.token_address
        , p.symbol
        , dt.num_rollups
        , dt.num_tfers
        , dt.abs_value_norm
        , dt.abs_value_norm * COALESCE(p.price_usd, b.eth_price) AS abs_volume_usd
        , dt.abs_value_norm * COALESCE(p.price_eth, 1) AS abs_volume_eth
        , dt.input_value_norm * COALESCE(p.price_usd, b.eth_price) AS input_volume_usd
        , dt.input_value_norm * COALESCE(p.price_eth, 1) AS input_volume_eth
        , dt.output_value_norm * COALESCE(p.price_usd, b.eth_price) AS output_volume_usd
        , dt.output_value_norm * COALESCE(p.price_eth, 1) AS output_volume_eth
    FROM daily_transfers dt
    inner join token_prices p ON dt.date = p.day AND dt.token_address = p.token_address
    LEFT JOIN token_prices b ON dt.date = b.day AND dt.token_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' -- using this to get price for missing ETH token
;