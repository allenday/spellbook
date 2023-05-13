{{ config(
    schema = 'archipelago_ethereum',
    alias ='base_trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}
{% set project_start_date='2022-6-20' %}

WITH
trade_events AS (
    SELECT * FROM {{ source('archipelago_ethereum','ArchipelagoMarket_evt_Trade') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% else %}
    WHERE evt_block_time >= '{{ project_start_date }}'
    {% endif %}
),

token_events AS (
    SELECT * FROM {{ source('archipelago_ethereum','ArchipelagoMarket_evt_TokenTrade') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% else %}
    WHERE evt_block_time >= '{{ project_start_date }}'
    {% endif %}
),

fee_events AS (
    SELECT
        evt_block_number,
        tradeid,
        sum(amount) FILTER (
            WHERE recipient NOT IN ("0xa76456bb6abc50fb38e17c042026bc27a95c3314", "0x1fc12c9f68a6b0633ba5897a40a8e61ed9274dc9")
        ) AS royalty_amount,
        sum(amount) FILTER (
            WHERE recipient IN ("0xa76456bb6abc50fb38e17c042026bc27a95c3314", "0x1fc12c9f68a6b0633ba5897a40a8e61ed9274dc9")
        ) AS platform_amount
    FROM {{ source('archipelago_ethereum','ArchipelagoMarket_evt_RoyaltyPayment') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% else %}
    WHERE evt_block_time >= '{{ project_start_date }}'
    {% endif %}
    GROUP BY evt_block_number, tradeid
)

SELECT
    date_trunc("day", trade.evt_block_time) AS block_date,
    trade.evt_block_time AS block_time,
    trade.evt_block_number AS block_number,
    trade.evt_tx_hash AS tx_hash,
    trade.contract_address AS project_contract_address,
    CAST(null AS varchar(5)) AS trade_category,
    "secondary" AS trade_type,
    trade.buyer,
    trade.seller,
    tok.tokenaddress AS nft_contract_address,
    tok.tokenid AS nft_token_id,
    1 AS nft_amount,
    trade.currency AS currency_contract,
    cast(trade.cost AS decimal(38)) AS price_raw,
    cast(coalesce(fee.platform_amount, 0) AS decimal(38)) AS platform_fee_amount_raw,
    cast(coalesce(fee.royalty_amount, 0) AS decimal(38)) AS royalty_fee_amount_raw,
    CAST(null AS varchar(5)) AS platform_fee_address,
    CAST(null AS varchar(5)) AS royalty_fee_address,
    trade.evt_index AS sub_tx_trade_id
FROM trade_events AS trade
INNER JOIN token_events AS tok
    ON
        trade.evt_block_number = tok.evt_block_number
        AND trade.tradeid = tok.tradeid
LEFT JOIN fee_events AS fee
    ON
        trade.evt_block_number = fee.evt_block_number
        AND trade.tradeid = fee.tradeid
