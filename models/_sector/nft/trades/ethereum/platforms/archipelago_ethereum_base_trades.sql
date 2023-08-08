{{ config(
    schema = 'archipelago_ethereum',
    alias ='base_trades',
    partition_by = {"field": "block_date"},
    materialized = 'view',
            unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}
{% set project_start_date='2022-6-20' %}

WITH
trade_events as (
    SELECT * FROM {{ source('archipelago_ethereum','ArchipelagoMarket_evt_Trade') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% else %}
    WHERE evt_block_time >= '{{project_start_date}}'
    {% endif %}
),
token_events as (
    SELECT * FROM {{ source('archipelago_ethereum','ArchipelagoMarket_evt_TokenTrade') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% else %}
    WHERE evt_block_time >= '{{project_start_date}}'
    {% endif %}
),
fee_events as (
    SELECT
        evt_block_number
        , tradeId
        , SUM(CASE WHEN recipient NOT IN ('0xa76456bb6abc50fb38e17c042026bc27a95c3314', '0x1fc12c9f68a6b0633ba5897a40a8e61ed9274dc9') THEN amount ELSE 0 END) AS royalty_amount
        , SUM(CASE WHEN recipient IN ('0xa76456bb6abc50fb38e17c042026bc27a95c3314', '0x1fc12c9f68a6b0633ba5897a40a8e61ed9274dc9') THEN amount ELSE 0 END) AS platform_amount
    FROM {{ source('archipelago_ethereum','ArchipelagoMarket_evt_RoyaltyPayment') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% else %}
    WHERE evt_block_time >= '{{project_start_date}}'
    {% endif %}
    GROUP BY evt_block_number, tradeId
)

SELECT
    TIMESTAMP_TRUNC(trade.evt_block_time, day) as block_date
    ,trade.evt_block_time as block_time
    ,trade.evt_block_number as block_number
    ,trade.evt_tx_hash as tx_hash
    ,trade.contract_address as project_contract_address
    ,CAST(null as STRING) as trade_category
    ,'secondary' as trade_type
    ,trade.buyer
    ,trade.seller
    ,tok.tokenAddress as nft_contract_address
    ,tok.tokenId as nft_token_id
    ,1 as nft_amount
    ,trade.currency as currency_contract
    ,cast(trade.cost as BIGNUMERIC) as price_raw
    ,cast(coalesce(fee.platform_amount,0) as BIGNUMERIC) as platform_fee_amount_raw
    ,cast(coalesce(fee.royalty_amount,0) as BIGNUMERIC) as royalty_fee_amount_raw
    ,CAST(null as STRING) as platform_fee_address
    ,CAST(null as STRING) as royalty_fee_address
    ,trade.evt_index as sub_tx_trade_id
FROM trade_events trade
INNER JOIN token_events tok
ON trade.evt_block_number = tok.evt_block_number
    AND trade.tradeId = tok.tradeId
LEFT JOIN fee_events fee
ON trade.evt_block_number = fee.evt_block_number
    AND trade.tradeId = fee.tradeId