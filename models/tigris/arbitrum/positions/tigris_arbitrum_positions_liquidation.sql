{{ config(
    alias = 'positions_liquidation'
    )
 }}

WITH

last_margin AS (
    SELECT
        xx.evt_block_time
        , xx.position_id
        , xy.margin
    FROM
        (
            SELECT
                MAX(evt_block_time) AS evt_block_time
                , position_id
            FROM
                {{ ref('tigris_arbitrum_positions_margin') }}
            GROUP BY 2
        ) AS xx
    INNER JOIN
        {{ ref('tigris_arbitrum_positions_margin') }} AS xy
        ON xx.evt_block_time = xy.evt_block_time
            AND xx.position_id = xy.position_id
)

, last_leverage AS (
    SELECT
        xx.evt_block_time
        , xx.position_id
        , xy.leverage
    FROM
        (
            SELECT
                MAX(evt_block_time) AS evt_block_time
                , position_id
            FROM
                {{ ref('tigris_arbitrum_positions_leverage') }}
            GROUP BY 2
        ) AS xx
    INNER JOIN
        {{ ref('tigris_arbitrum_positions_leverage') }} AS xy
        ON xx.evt_block_time = xy.evt_block_time
            AND xx.position_id = xy.position_id
)

SELECT
    lp.*
    , last_margin.margin
    , last_leverage.leverage
FROM
    {{ ref('tigris_arbitrum_events_liquidate_position') }} AS lp
INNER JOIN
    last_margin
    ON lp.position_id = last_margin.position_id
INNER JOIN
    last_leverage
    ON lp.position_id = last_leverage.position_id
