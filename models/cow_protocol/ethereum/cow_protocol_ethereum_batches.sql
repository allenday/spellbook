{{  config(
        alias='batches',
        materialized='incremental',
        partition_by = ['block_date'],
        unique_key = ['tx_hash'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith", "gentrexha"]\') }}'
    )
}}

WITH
-- Find the PoC Query here: https: / /dune.com/queries/1290518
batch_counts AS (
    SELECT try_cast(date_trunc('day', s.evt_block_time) AS date) AS block_date,
           s.evt_block_time,
           s.evt_tx_hash,
           solver,
           name,
           sum(
               CASE
                   WHEN selector != '0x2e1a7d4d' -- unwrap
                    AND selector != '0x095ea7b3' -- approval
                       THEN 1
                   ELSE 0
                END)                                                AS dex_swaps,
           sum(CASE WHEN selector = '0x2e1a7d4d' THEN 1 ELSE 0 END) AS unwraps,
           sum(CASE WHEN selector = '0x095ea7b3' THEN 1 ELSE 0 END) AS token_approvals
    FROM {{ source('gnosis_protocol_v2_ethereum', 'GPv2Settlement_evt_Settlement') }} s
        left outer join {{ source('gnosis_protocol_v2_ethereum', 'GPv2Settlement_evt_Interaction') }} i
            ON i.evt_tx_hash = s.evt_tx_hash
            {% if is_incremental() %}
            AND i.evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        join cow_protocol_ethereum.solvers
            ON solver = address
    {% if is_incremental() %}
    WHERE s.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    GROUP BY s.evt_tx_hash, solver, s.evt_block_time, name
),

batch_values AS (
    SELECT
        tx_hash,
        count(*)        AS num_trades,
        sum(usd_value)  AS batch_value,
        sum(fee_usd)    AS fee_value,
        price           AS eth_price
    FROM {{ ref('cow_protocol_ethereum_trades') }}
        left outer join {{ source('prices', 'usd') }} AS p
            ON p.contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            AND p.minute = date_trunc('minute', block_time)
            AND blockchain = 'ethereum'
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    GROUP BY tx_hash, price
),

combined_batch_info AS (
    SELECT
        block_date,
        evt_block_time                                 AS block_time,
        num_trades,
        CASE
            WHEN (
              name ilike '%1inch'
               OR name ilike '%ParaSwap'
               OR name ilike '%0x'
               OR name = 'Legacy'
              )
               THEN (
                SELECT count(*)
                FROM {{ ref('dex_trades') }}
                where tx_hash = evt_tx_hash
                AND blockchain = 'ethereum'
              )
            ELSE dex_swaps
        END                                              AS dex_swaps,
        batch_value,
        solver                                           AS solver_address,
        evt_tx_hash                                      AS tx_hash,
        gas_price,
        gas_used,
        (gas_price * gas_used * eth_price) / pow(10, 18) AS tx_cost_usd,
        fee_value,
        length(data)::decimal / 1024                     AS call_data_size,
        unwraps,
        token_approvals
    FROM batch_counts b
        join batch_values t
            ON b.evt_tx_hash = t.tx_hash
        inner join {{ source('ethereum', 'transactions') }} tx
            ON evt_tx_hash = hash
            {% if is_incremental() %}
            AND block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
    where num_trades > 0 --! Exclude Withdraw Batches
)

SELECT * FROM combined_batch_info
