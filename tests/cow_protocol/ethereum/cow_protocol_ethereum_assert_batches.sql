-- Try it out here: https://dune.com/queries/1389623
WITH batches_with_trades AS (
    SELECT
        s.evt_tx_hash,
        s.evt_block_time
    FROM
        {{ source('gnosis_protocol_v2_ethereum','GPv2Settlement_evt_Trade') }} AS t
    INNER JOIN
        {{ source('gnosis_protocol_v2_ethereum','GPv2Settlement_evt_Settlement') }} AS s
        ON s.evt_tx_hash = t.evt_tx_hash
    GROUP BY s.evt_tx_hash, s.evt_block_time
)

SELECT evt_tx_hash FROM batches_with_trades
WHERE
    evt_tx_hash NOT IN (
        SELECT tx_hash
        FROM {{ ref('cow_protocol_ethereum_batches' ) }}
    )
    -- The reference table is only refreshed once in a while,
    -- so we impose a time constraint on this test.
    AND evt_block_time < date(now()) - INTERVAL '1 day'
