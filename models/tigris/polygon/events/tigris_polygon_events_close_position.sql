{{ config(
    alias = 'events_close_position',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'evt_index', 'position_id', 'trader', 'price', 'payout', 'perc_closed']
    )
}} 

WITH

close_position_v1 AS (
    SELECT
        tc.evt_tx_hash
        , tc.evt_index
        , tc.evt_block_time
        , tc._id AS position_id
        , op.trader
        , date_trunc('day', tc.evt_block_time) AS day
        , tc._closePrice / 1e18 AS price
        , tc._payout / 1e18 AS payout
        , tc._percent / 100 AS perc_closed
    FROM
        {{ source('tigristrade_polygon', 'Tradingv1_evt_PositionClosed') }} AS tc
    INNER JOIN
        {{ ref('tigris_polygon_events_open_position') }} AS op
        ON tc._id = op.position_id
            AND op.version = 'v1'
    {% if is_incremental() %}
        WHERE tc.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, close_position_v2 AS (
    SELECT
        tc.evt_tx_hash
        , tc.evt_index
        , tc.evt_block_time
        , tc._id AS position_id
        , op.trader
        , date_trunc('day', tc.evt_block_time) AS day
        , tc._closePrice / 1e18 AS price
        , tc._payout / 1e18 AS payout
        , tc._percent / 100 AS perc_closed
    FROM
        {{ source('tigristrade_polygon', 'TradingV2_evt_PositionClosed') }} AS tc
    INNER JOIN
        {{ ref('tigris_polygon_events_open_position') }} AS op
        ON tc._id = op.position_id
            AND op.version = 'v2'
    {% if is_incremental() %}
        WHERE tc.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, close_position_v3 AS (
    SELECT
        tc.evt_tx_hash
        , tc.evt_index
        , tc.evt_block_time
        , tc._id AS position_id
        , op.trader
        , date_trunc('day', tc.evt_block_time) AS day
        , tc._closePrice / 1e18 AS price
        , tc._payout / 1e18 AS payout
        , tc._percent / 100 AS perc_closed
    FROM
        {{ source('tigristrade_polygon', 'TradingV3_evt_PositionClosed') }} AS tc
    INNER JOIN
        {{ ref('tigris_polygon_events_open_position') }} AS op
        ON tc._id = op.position_id
            AND op.version = 'v3'
    {% if is_incremental() %}
        WHERE tc.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)


, close_position_v4 AS (
    SELECT
        tc.evt_tx_hash
        , tc.evt_index
        , tc.evt_block_time
        , tc._id AS position_id
        , op.trader
        , date_trunc('day', tc.evt_block_time) AS day
        , tc._closePrice / 1e18 AS price
        , tc._payout / 1e18 AS payout
        , tc._percent / 100 AS perc_closed
    FROM
        {{ source('tigristrade_polygon', 'TradingV4_evt_PositionClosed') }} AS tc
    INNER JOIN
        {{ ref('tigris_polygon_events_open_position') }} AS op
        ON tc._id = op.position_id
            AND op.version = 'v4'
    {% if is_incremental() %}
        WHERE tc.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, close_position_v5 AS (
    SELECT
        evt_tx_hash
        , evt_index
        , evt_block_time
        , _id AS position_id
        , _trader AS trader
        , date_trunc('day', evt_block_time) AS day
        , _closePrice / 1e18 AS price
        , _payout / 1e18 AS payout
        , _percent / 1e8 AS perc_closed
    FROM
        {{ source('tigristrade_polygon', 'TradingV5_evt_PositionClosed') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, close_position_v6 AS (
    SELECT
        evt_tx_hash
        , evt_index
        , evt_block_time
        , _id AS position_id
        , _trader AS trader
        , date_trunc('day', evt_block_time) AS day
        , _closePrice / 1e18 AS price
        , _payout / 1e18 AS payout
        , _percent / 1e8 AS perc_closed
    FROM
        {{ source('tigristrade_polygon', 'TradingV6_evt_PositionClosed') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, close_position_v7 AS (
    SELECT
        evt_tx_hash
        , evt_index
        , evt_block_time
        , _id AS position_id
        , _trader AS trader
        , date_trunc('day', evt_block_time) AS day
        , _closePrice / 1e18 AS price
        , _payout / 1e18 AS payout
        , _percent / 1e8 AS perc_closed
    FROM
        {{ source('tigristrade_polygon', 'TradingV7_evt_PositionClosed') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, close_position_v8 AS (
    SELECT *
    FROM
        (
            SELECT
                evt_tx_hash
                , evt_index
                , evt_block_time
                , _id AS position_id
                , _trader AS trader
                , date_trunc('day', evt_block_time) AS day
                , _closePrice / 1e18 AS price
                , _payout / 1e18 AS payout
                , _percent / 1e8 AS perc_closed
            FROM
                {{ source('tigristrade_polygon', 'TradingV8_evt_PositionClosed') }}
            {% if is_incremental() %}
                WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
            {% endif %}
        ) AS t
    WHERE t.evt_tx_hash NOT IN ('0x561cde89720f8af596bf8958dd96339d8b3923094d6d27dd8bf14f5326c9ae25', '0x17e49a19c4feaf014bf485ee2277bfa09375bde9931da9a95222de7a1e704d70', '0x146e22e33c8218ac8c70502b292bbc6d9334983135a1e70ffe0125784bfdcc91')
)

SELECT
    *
    , 'v1' AS version
FROM close_position_v1

UNION ALL

SELECT
    *
    , 'v2' AS version
FROM close_position_v2

UNION ALL

SELECT
    *
    , 'v3' AS version
FROM close_position_v3

UNION ALL

SELECT
    *
    , 'v4' AS version
FROM close_position_v4

UNION ALL

SELECT
    *
    , 'v5' AS version
FROM close_position_v5

UNION ALL

SELECT
    *
    , 'v6' AS version
FROM close_position_v6

UNION ALL

SELECT
    *
    , 'v7' AS version
FROM close_position_v7

UNION ALL

SELECT
    *
    , 'v8' AS version
FROM close_position_v8
