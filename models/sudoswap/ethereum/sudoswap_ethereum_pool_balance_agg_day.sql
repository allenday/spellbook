{{ config(
        alias = 'pool_balance_changes',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'pool_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "sudoswap",
                                    \'["niftytable", "0xRob"]\') }}'
        )
}}

{% set project_start_date = '2022-04-23' %}

WITH
  pools AS (
    SELECT
         pool_address
        ,nft_contract_address
    FROM {{ ref('sudoswap_ethereum_pool_creations') }}
  ),

  erc721_deltas AS (
    SELECT
      date_trunc('day', et.evt_block_time) AS day,
      pool_address,
      SUM(CASE WHEN et.to = p.pool_address THEN 1 ELSE -1 END) AS nft_balance_change
    FROM
      {{ source('erc721_ethereum','evt_transfer') }} et
      INNER JOIN pools p ON p.nft_contract_address = et.contract_address
      AND (et.to = p.pool_address OR et.from = p.pool_address)
    {% if NOT is_incremental() %}
    WHERE et.evt_block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE et.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    GROUP BY
      1,2
  ),

  eth_deltas AS (
  SELECT
  day
  , pool_address
  , coalesce(sum(eth_balance_in),0) - coalesce(sum(eth_balance_out),0) AS eth_balance_change
  from(
    SELECT * from (
        SELECT
          date_trunc('day',tr.block_time) AS day
          , pool_address
          , SUM(tr.value / 1e18) AS eth_balance_in
          , 0 AS eth_balance_out
        FROM
          {{ source('ethereum','traces') }} tr
          INNER JOIN pools pc ON pc.pool_address = tr.to
        WHERE tr.success = true
          AND tr.type = 'call'
          AND (
            tr.call_type NOT IN ('delegatecall', 'callcode', 'staticcall')
            OR tr.call_type IS NULL
          )
          {% if NOT is_incremental() %}
          AND tr.block_time > '{{project_start_date}}'
          {% endif %}
          {% if is_incremental() %}
          AND tr.block_time >= date_trunc("day", now() - interval '1 week')
          {% endif %}
        GROUP BY
          1,2
    ) foo
    union all
    SELECT * from (
        SELECT
          date_trunc('day',tr.block_time) AS day
          , pool_address
          , 0 AS eth_balance_in
          , SUM(tr.value / 1e18) AS eth_balance_out
        FROM
          {{ source('ethereum','traces') }} tr
          INNER JOIN pools pc ON pc.pool_address = tr.from
        WHERE tr.success = true
          AND tr.type = 'call'
          AND (
            tr.call_type NOT IN ('delegatecall', 'callcode', 'staticcall')
            OR tr.call_type IS NULL
          )
          {% if NOT is_incremental() %}
          AND tr.block_time > '{{project_start_date}}'
          {% endif %}
          {% if is_incremental() %}
          AND tr.block_time >= date_trunc("day", now() - interval '1 week')
          {% endif %}
        GROUP BY
          1,2
    ) foo
  ) daily
  GROUP BY 1,2
  )

SELECT
    COALESCE(e.day, n.day) AS day,
    COALESCE(e.pool_address,n.pool_address) AS pool_address,
    COALESCE(e.eth_balance_change, 0) AS eth_balance_change,
    COALESCE(n.nft_balance_change,0) AS nft_balance_change
FROM eth_deltas e
FULL JOIN erc721_deltas n
    ON e.day = n.day
    AND e.pool_address = n.pool_address
;
