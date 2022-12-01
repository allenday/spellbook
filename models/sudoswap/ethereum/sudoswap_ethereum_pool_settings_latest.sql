{{ config(
        alias = 'pool_settings_latest',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "sudoswap",
                                    \'["0xRob"]\') }}'
        )
}}

{% set project_start_date = '2022-04-23' %}

with
  latest_pool_fee AS (
    SELECT
        pool_address
        , pool_fee
        , update_time
        FROM (
            SELECT
                contract_address AS pool_address
                ,newFee AS pool_fee
                ,evt_block_time AS update_time
                ,row_number() over (partition by contract_address order by evt_block_number desc, tx.index desc) AS ordering
            FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_evt_FeeUpdate') }} evt
            INNER JOIN {{ source('ethereum','transactions') }} tx ON tx.block_time = evt.evt_block_time
            AND tx.hash = evt.evt_tx_hash
            {% if NOT is_incremental() %}
            AND tx.block_time >= '{{project_start_date}}'
            AND evt.evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND tx.block_time >= date_trunc("day", now() - interval '1 week')
            AND evt.evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) foo
    WHERE ordering = 1
)

, latest_delta AS (
    SELECT
        pool_address
        , delta
        , update_time
        FROM (
            SELECT
                contract_address AS pool_address
                ,newDelta / 1e18 AS delta
                ,evt_block_time AS update_time
                ,row_number() over (partition by contract_address order by evt_block_number desc, tx.index desc) AS ordering
            FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_evt_DeltaUpdate') }} evt
            INNER JOIN {{ source('ethereum','transactions') }} tx ON tx.block_time = evt.evt_block_time
            AND tx.hash = evt.evt_tx_hash
            {% if NOT is_incremental() %}
            AND tx.block_time >= '{{project_start_date}}'
            AND evt.evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND tx.block_time >= date_trunc("day", now() - interval '1 week')
            AND evt.evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) foo
    WHERE ordering = 1
)

, latest_spot_price AS (
    SELECT
        pool_address
        , spot_price
        , update_time
        FROM (
            SELECT
                contract_address AS pool_address
                ,newSpotPrice / 1e18 AS spot_price
                ,evt_block_time AS update_time
                ,row_number() over (partition by contract_address order by evt_block_number desc, tx.index desc) AS ordering
            FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_evt_SpotPriceUpdate') }} evt
            INNER JOIN {{ source('ethereum','transactions') }} tx ON tx.block_time = evt.evt_block_time
            AND tx.hash = evt.evt_tx_hash
            {% if NOT is_incremental() %}
            AND tx.block_time >= '{{project_start_date}}'
            AND evt.evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND tx.block_time >= date_trunc("day", now() - interval '1 week')
            AND evt.evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) foo
    WHERE ordering = 1
)

, latest_settings AS (
    SELECT
        coalesce(t1.pool_address, t2.pool_address, t3.pool_address) AS pool_address
        ,pool_fee
        ,delta
        ,spot_price
        ,coalesce(t1.update_time, t2.update_time, t3.update_time) AS latest_update_time
    from latest_spot_price t1
    full join latest_delta t2 on t1.pool_address = t2.pool_address
    full join latest_pool_fee t3 on t1.pool_address = t3.pool_address or t2.pool_address = t3.pool_address
)

, initial_settings AS (
    SELECT
      pool_address,
      bonding_curve,
      spot_price,
      delta,
      pool_fee,
      creation_block_time
    FROM
      {{ ref('sudoswap_ethereum_pool_creations') }}
    {% if is_incremental() %}
    WHERE creation_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

-- incremental update:
-- we need to backfill columns from the existing data in order to have full rows
{% if is_incremental() %}
, full_settings_backfilled AS (
    SELECT * from(
        SELECT
         coalesce(t1.pool_address,t3.pool_address) AS pool_address
        ,coalesce(t2.bonding_curve, t3.bonding_curve) AS bonding_curve
        ,coalesce(t1.pool_fee, t2.pool_fee, t3.pool_fee) AS pool_fee
        ,coalesce(t1.delta, t2.delta, t3.delta) AS delta
        ,coalesce(t1.spot_price, t2.spot_price, t3.spot_price) AS spot_price
        ,coalesce(t1.latest_update_time,t3.creation_block_time) AS latest_update_time
        from latest_settings t1
        full outer join initial_settings t3
            ON t1.pool_address = t3.pool_address
        LEFT JOIN {{ this }} t2
            ON t1.pool_address = t2.pool_address
    ) foo
    where bonding_curve is NOT null --temp hack to exclude updates form erc20 pools
)
{% endif %}


-- This happens on a full refresh, no backfill necesarry.
{% if NOT is_incremental() %}
, full_settings_backfilled AS (
    SELECT
     coalesce(new.pool_address,old.pool_address) AS pool_address
    ,coalesce(old.bonding_curve) AS bonding_curve
    ,coalesce(new.pool_fee,old.pool_fee) AS pool_fee
    ,coalesce(new.delta, old.delta) AS delta
    ,coalesce(new.spot_price,old.spot_price) AS spot_price
    ,coalesce(new.latest_update_time,old.creation_block_time) AS latest_update_time
    from initial_settings old
    LEFT JOIN latest_settings new
    ON old.pool_address = new.pool_address
)
{% endif %}


SELECT * from full_settings_backfilled
;
