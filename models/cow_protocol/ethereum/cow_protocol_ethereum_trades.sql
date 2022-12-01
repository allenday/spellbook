{{  config(
        alias='trades',
        materialized='incremental',
        partition_by = ['block_date'],
        unique_key = ['tx_hash', 'order_uid', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge'
    )
}}

-- Find the PoC Query here: https: / /dune.com/queries/1283229
WITH
-- First subquery joins buy AND sell token prices FROM prices.usd
-- Also deducts fee FROM sell amount
trades_with_prices AS (
    SELECT try_cast(date_trunc('day', evt_block_time) AS date) AS block_date,
           evt_block_time            AS block_time,
           evt_tx_hash               AS tx_hash,
           evt_index,
           settlement.contract_address          AS project_contract_address,
           owner                     AS trader,
           orderUid                  AS order_uid,
           sellToken                 AS sell_token,
           buyToken                  AS buy_token,
           (sellAmount - feeAmount)  AS sell_amount,
           buyAmount                 AS buy_amount,
           feeAmount                 AS fee_amount,
           ps.price                  AS sell_price,
           pb.price                  AS buy_price
    FROM {{ source('gnosis_protocol_v2_ethereum', 'GPv2Settlement_evt_Trade') }} settlement
             LEFT OUTER JOIN {{ source('prices', 'usd') }} AS ps
                             ON sellToken = ps.contract_address
                                 AND ps.minute = date_trunc('minute', evt_block_time)
                                 AND ps.blockchain = 'ethereum'
                                 {% if is_incremental() %}
                                 AND ps.minute >= date_trunc("day", now() - INTERVAL '1 week')
                                 {% endif %}
             LEFT OUTER JOIN {{ source('prices', 'usd') }} AS pb
                             ON pb.contract_address = (
                                 CASE
                                     WHEN buyToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                                         THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                     ELSE buyToken
                                     END)
                                 AND pb.minute = date_trunc('minute', evt_block_time)
                                 AND pb.blockchain = 'ethereum'
                                 {% if is_incremental() %}
                                 AND pb.minute >= date_trunc("day", now() - INTERVAL '1 week')
                                 {% endif %}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - INTERVAL '1 week')
    {% endif %}
),
-- Second subquery gets token symbol AND decimals FROM tokens.erc20 (to display units bought AND sold)
trades_with_token_units AS (
    SELECT block_date,
           block_time,
           tx_hash,
           evt_index,
           project_contract_address,
           order_uid,
           trader,
           sell_token                        AS sell_token_address,
           (CASE
                WHEN ts.symbol IS NULL THEN sell_token
                ELSE ts.symbol
               END)                          AS sell_token,
           buy_token                         AS buy_token_address,
           (CASE
                WHEN tb.symbol IS NULL THEN buy_token
                WHEN buy_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'ETH'
                ELSE tb.symbol
               END)                          AS buy_token,
           sell_amount / pow(10, ts.decimals) AS units_sold,
           sell_amount                       AS atoms_sold,
           buy_amount / pow(10, tb.decimals)  AS units_bought,
           buy_amount                        AS atoms_bought,
           -- We use sell value WHEN possible AND buy value WHEN NOT
           fee_amount / pow(10, ts.decimals)  AS fee,
           fee_amount                        AS fee_atoms,
           sell_price,
           buy_price
    FROM trades_with_prices
             LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20') }} ts
                             ON ts.contract_address = sell_token
             LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20') }} tb
                             ON tb.contract_address =
                                (CASE
                                     WHEN buy_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                                         THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                     ELSE buy_token
                                    END)
),
-- This, independent, aggregation defines a mapping of order_uid AND trade
-- TODO - create a view FOR the following block mapping uid to app_data
order_ids AS (
    SELECT evt_tx_hash, collect_list(orderUid) AS order_ids
    FROM (  SELECT orderUid, evt_tx_hash, evt_index
            FROM {{ source('gnosis_protocol_v2_ethereum', 'GPv2Settlement_evt_Trade') }}
             {% if is_incremental() %}
             where evt_block_time >= date_trunc("day", now() - INTERVAL '1 week')
             {% endif %}
                     sort BY evt_index
         ) AS _
    GROUP BY evt_tx_hash
),

exploded_order_ids AS (
    SELECT evt_tx_hash, posexplode(order_ids)
    FROM order_ids
),

reduced_order_ids AS (
    SELECT
        col AS order_id,
        -- This is a dirty hack!
        collect_list(evt_tx_hash)[0] AS evt_tx_hash,
        collect_list(pos)[0] AS pos
    FROM exploded_order_ids
    GROUP BY order_id
),

trade_data AS (
    SELECT call_tx_hash,
           posexplode(trades)
    FROM {{ source('gnosis_protocol_v2_ethereum', 'GPv2Settlement_call_settle') }}
    where call_success = true
    {% if is_incremental() %}
    AND call_block_time >= date_trunc("day", now() - INTERVAL '1 week')
    {% endif %}
),

uid_to_app_id AS (
    SELECT
        order_id AS uid,
        get_json_object(trades.col, '$.appData') AS app_data,
        get_json_object(trades.col, '$.receiver') AS receiver
    FROM reduced_order_ids order_ids
             JOIN trade_data trades
                  ON evt_tx_hash = call_tx_hash
                      AND order_ids.pos = trades.pos
),

valued_trades AS (
    SELECT block_date,
           block_time,
           tx_hash,
           evt_index,
           project_contract_address,
           order_uid,
           trader,
           sell_token_address,
           sell_token,
           buy_token_address,
           buy_token,
           CASE
                 WHEN lower(buy_token) > lower(sell_token) THEN concat(sell_token, '-', buy_token)
                 ELSE concat(buy_token, '-', sell_token)
               END AS token_pair,
           units_sold,
           atoms_sold,
           units_bought,
           atoms_bought,
           (CASE
                WHEN sell_price IS NOT NULL THEN
                    -- Choose the larger of two prices WHEN both NOT NULL.
                    CASE
                        WHEN buy_price IS NOT NULL AND buy_price * units_bought > sell_price * units_sold
                            THEN buy_price * units_bought
                        ELSE sell_price * units_sold
                        END
                WHEN sell_price IS NULL AND buy_price IS NOT NULL THEN buy_price * units_bought
                ELSE NULL::numeric
               END)                                        AS usd_value,
           buy_price,
           buy_price * units_bought                        AS buy_value_usd,
           sell_price,
           sell_price * units_sold                         AS sell_value_usd,
           fee,
           fee_atoms,
           (CASE
                WHEN sell_price IS NOT NULL THEN
                    CASE
                        WHEN buy_price IS NOT NULL AND buy_price * units_bought > sell_price * units_sold
                            THEN buy_price * units_bought * fee / units_sold
                        ELSE sell_price * fee
                        END
                WHEN sell_price IS NULL AND buy_price IS NOT NULL
                    THEN buy_price * units_bought * fee / units_sold
                ELSE NULL::numeric
               END)                                        AS fee_usd,
           app_data,
           receiver
    FROM trades_with_token_units
             JOIN uid_to_app_id
                  ON uid = order_uid
)

SELECT * FROM valued_trades
