{{ config(
    schema = 'tigris_v2_polygon',
    alias = 'trades',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trade_type']
    )
}}

WITH

open_position AS (
    SELECT
        day,
        evt_block_time,
        evt_index,
        evt_tx_hash,
        position_id,
        price,
        margin AS new_margin,
        leverage,
        volume_usd,
        margin_asset,
        pair,
        direction,
        referral,
        trader,
        margin AS margin_change,
        version,
        'open_position' AS trade_type
    FROM {{ ref('tigris_v2_polygon_events_open_position') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

limit_order AS (
    SELECT
        day,
        evt_block_time,
        evt_index,
        evt_tx_hash,
        position_id,
        price,
        margin AS new_margin,
        leverage,
        volume_usd,
        margin_asset,
        pair,
        direction,
        referral,
        trader,
        margin AS margin_change,
        version,
        'limit_order' AS trade_type
    FROM {{ ref('tigris_v2_polygon_events_limit_order') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

close_position AS (
    SELECT
        date_trunc('day', c.evt_block_time) AS day,
        c.evt_block_time,
        c.evt_index,
        c.evt_tx_hash,
        c.position_id,
        c.price,
        c.new_margin AS new_margin,
        c.leverage,
        c.payout * c.leverage AS volume_usd,
        COALESCE(op.margin_asset, lo.margin_asset) AS margin_asset,
        COALESCE(op.pair, lo.pair) AS pair,
        COALESCE(op.direction, lo.direction) AS direction,
        COALESCE(op.referral, lo.referral) AS referral,
        c.trader,
        c.payout AS margin_change,
        c.version,
        'close_position' AS trade_type
    FROM
        {{ ref('tigris_v2_polygon_positions_close') }} AS c
    LEFT JOIN
        open_position AS op
        ON c.position_id = op.position_id
    LEFT JOIN
        limit_order AS lo
        ON c.position_id = lo.position_id
    {% if is_incremental() %}
        WHERE c.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

),

liquidate_position AS (
    SELECT
        lp.day,
        lp.evt_block_time,
        lp.evt_index,
        lp.evt_tx_hash,
        lp.position_id,
        CAST(NULL AS double) AS price,
        0 AS new_margin,
        lp.leverage,
        lp.margin * lp.leverage AS volume_usd,
        COALESCE(op.margin_asset, lo.margin_asset) AS margin_asset,
        COALESCE(op.pair, lo.pair) AS pair,
        COALESCE(op.direction, lo.direction) AS direction,
        COALESCE(op.referral, lo.referral) AS referral,
        lp.trader,
        lp.margin AS margin_change,
        lp.version,
        'liquidate_position' AS trade_type
    FROM
        {{ ref('tigris_v2_polygon_positions_liquidation') }} AS lp
    LEFT JOIN
        open_position AS op
        ON lp.position_id = op.position_id
    LEFT JOIN
        limit_order AS lo
        ON lp.position_id = lo.position_id
    {% if is_incremental() %}
        WHERE lp.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

add_margin AS (
    SELECT
        am.day,
        am.evt_block_time,
        am.evt_index,
        am.evt_tx_hash,
        am.position_id,
        am.price,
        am.margin AS new_margin,
        am.leverage,
        am.margin_change * am.leverage AS volume_usd,
        COALESCE(op.margin_asset, lo.margin_asset) AS margin_asset,
        COALESCE(op.pair, lo.pair) AS pair,
        COALESCE(op.direction, lo.direction) AS direction,
        COALESCE(op.referral, lo.referral) AS referral,
        am.trader,
        am.margin_change,
        am.version,
        'add_to_position' AS trade_type
    FROM
        (
            SELECT
                tmp.*,
                l.leverage
            FROM
                (
                    SELECT
                        MIN(l.evt_block_time) AS latest_leverage_time,
                        am.day,
                        am.evt_block_time,
                        am.evt_tx_hash,
                        am.evt_index,
                        am.position_id,
                        am.price,
                        am.margin,
                        am.margin_change,
                        am.version,
                        am.trader
                    FROM
                        {{ ref('tigris_v2_polygon_events_add_margin') }} AS am
                    INNER JOIN
                        {{ ref('tigris_v2_polygon_positions_leverage') }} AS l
                        ON
                            am.position_id = l.position_id
                            AND am.evt_block_time > l.evt_block_time
                            {% if is_incremental() %}
                                AND l.evt_block_time >= date_trunc('day', now() - interval '1 week')
                            {% endif %}
                    {% if is_incremental() %}
                        WHERE am.evt_block_time >= date_trunc('day', now() - interval '1 week')
                    {% endif %}
                    GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
                ) AS tmp
            INNER JOIN
                {{ ref('tigris_v2_polygon_positions_leverage') }} AS l
                ON
                    tmp.position_id = l.position_id
                    AND tmp.latest_leverage_time = l.evt_block_time
                    {% if is_incremental() %}
                        AND l.evt_block_time >= date_trunc('day', now() - interval '1 week')
                    {% endif %}
        ) AS am
    LEFT JOIN
        open_position AS op
        ON am.position_id = op.position_id
    LEFT JOIN
        limit_order AS lo
        ON am.position_id = lo.position_id
),

modify_margin AS (
    SELECT
        mm.day,
        mm.evt_block_time,
        mm.evt_index,
        mm.evt_tx_hash,
        mm.position_id,
        CAST(NULL AS double) AS price,
        mm.margin AS new_margin,
        mm.leverage,
        mm.margin_change * mm.leverage AS volume_usd,
        COALESCE(op.margin_asset, lo.margin_asset) AS margin_asset,
        COALESCE(op.pair, lo.pair) AS pair,
        COALESCE(op.direction, lo.direction) AS direction,
        COALESCE(op.referral, lo.referral) AS referral,
        mm.trader,
        mm.margin_change,
        mm.version,
        CASE WHEN mm.modify_type = TRUE THEN 'add_margin' ELSE 'remove_margin' END AS trade_type
    FROM
        {{ ref('tigris_v2_polygon_events_modify_margin') }} AS mm
    LEFT JOIN
        open_position AS op
        ON mm.position_id = op.position_id
    LEFT JOIN
        limit_order AS lo
        ON mm.position_id = lo.position_id
    {% if is_incremental() %}
        WHERE mm.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
)

SELECT
    'polygon' AS blockchain,
    *
FROM open_position

UNION ALL

SELECT
    'polygon' AS blockchain,
    *
FROM close_position

UNION ALL

SELECT
    'polygon' AS blockchain,
    *
FROM liquidate_position

UNION ALL

SELECT
    'polygon' AS blockchain,
    *
FROM add_margin

UNION ALL

SELECT
    'polygon' AS blockchain,
    *
FROM modify_margin

UNION ALL

SELECT
    'polygon' AS blockchain,
    *
FROM limit_order;
