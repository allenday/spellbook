{{ config(
    schema = 'tigris_v1_polygon',
    alias = 'positions_liquidation'
    )
 }}

WITH

last_margin AS (
    SELECT
        evt_block_time,
        position_id,
        version,
        margin,
        evt_index
    FROM
        (
            SELECT
                ROW_NUMBER() OVER (PARTITION BY position_id, version ORDER BY evt_index DESC) AS rank_,
                *
            FROM
                (
                    SELECT
                        xx.evt_block_time,
                        xx.position_id,
                        xx.version,
                        xy.margin,
                        xy.evt_index
                    FROM
                        (
                            SELECT
                                MAX(evt_block_time) AS evt_block_time,
                                position_id,
                                version
                            FROM
                                {{ ref('tigris_v1_polygon_positions_margin') }}
                            GROUP BY 2, 3
                        ) AS xx
                    INNER JOIN
                        {{ ref('tigris_v1_polygon_positions_margin') }} AS xy
                        ON
                            xx.evt_block_time = xy.evt_block_time
                            AND xx.position_id = xy.position_id
                            AND xx.version = xy.version
                ) AS tmp
        ) AS tmp_2
    WHERE rank_ = 1
),

last_leverage AS (
    SELECT
        evt_block_time,
        position_id,
        version,
        leverage,
        evt_index
    FROM
        (
            SELECT
                ROW_NUMBER() OVER (PARTITION BY position_id, version ORDER BY evt_index DESC) AS rank_,
                *
            FROM
                (
                    SELECT
                        xx.evt_block_time,
                        xx.position_id,
                        xx.version,
                        xy.leverage,
                        xy.evt_index
                    FROM
                        (
                            SELECT
                                MAX(evt_block_time) AS evt_block_time,
                                position_id,
                                version
                            FROM
                                {{ ref('tigris_v1_polygon_positions_leverage') }}
                            GROUP BY 2, 3
                        ) AS xx
                    INNER JOIN
                        {{ ref('tigris_v1_polygon_positions_leverage') }} AS xy
                        ON
                            xx.evt_block_time = xy.evt_block_time
                            AND xx.position_id = xy.position_id
                            AND xx.version = xy.version
                ) AS tmp
        ) AS tmp_2
    WHERE rank_ = 1
)

SELECT
    lp.*,
    lm.margin,
    ll.leverage
FROM
    {{ ref('tigris_v1_polygon_events_liquidate_position') }} AS lp
INNER JOIN
    last_margin AS lm
    ON
        lp.position_id = lm.position_id
        AND lp.version = lm.version
INNER JOIN
    last_leverage AS ll
    ON
        lp.position_id = ll.position_id
        AND lp.version = ll.version;
