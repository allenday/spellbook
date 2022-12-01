{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash','evt_index','nft_contract_address','token_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "seaport",
                            \'["sohawk"]\') }}'
    )
}}

{% set c_native_token_address = "0x0000000000000000000000000000000000000000" %}
{% set c_alternative_token_address = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" %}
{% set c_native_symbol = "ETH" %}
{% set c_seaport_first_date = "2022-06-01" %}

with source_ethereum_transactions AS (
    SELECT *
    FROM {{ source('ethereum','transactions') }}
    {% if NOT is_incremental() %}
    where block_time >= date '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
    where block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,ref_seaport_ethereum_base_pairs AS (
      SELECT *
      FROM {{ ref('seaport_ethereum_base_pairs') }}
      where 1=1
      {% if is_incremental() %}
            AND block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}
)
,ref_tokens_nft AS (
    SELECT *
    FROM {{ ref('tokens_nft') }}
    where blockchain = 'ethereum'
)
,ref_tokens_erc20 AS (
    SELECT *
    FROM {{ ref('tokens_erc20') }}
    where blockchain = 'ethereum'
)
,ref_nft_aggregators AS (
    SELECT *
    FROM {{ ref('nft_aggregators') }}
    where blockchain = 'ethereum'
)
,source_prices_usd AS (
    SELECT *
    FROM {{ source('prices', 'usd') }}
    where blockchain = 'ethereum'
    {% if NOT is_incremental() %}
      AND minute >= date '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
      AND minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,iv_base_pairs_priv AS (
  SELECT a.block_time
        ,a.block_number
        ,a.tx_hash
        ,a.evt_index
        ,a.sub_type
        ,a.sub_idx
        ,a.offer_first_item_type
        ,a.consideration_first_item_type
        ,a.sender
        ,a.receiver
        ,a.zone
        ,a.token_contract_address
        ,a.original_amount
        ,a.item_type
        ,a.token_id
        ,a.platform_contract_address
        ,a.offer_cnt
        ,a.consideration_cnt
        ,a.is_private
        ,a.eth_erc_idx
        ,a.nft_cnt
        ,a.erc721_cnt
        ,a.erc1155_cnt
        ,a.order_type
        ,a.is_price
        ,a.is_netprice
        ,a.is_platform_fee
        ,a.is_creator_fee
        ,a.creator_fee_idx
        ,a.is_traded_nft
        ,a.is_moved_nft
  FROM ref_seaport_ethereum_base_pairs a
  where 1=1
    AND NOT a.is_private
  union all
  SELECT a.block_time
        ,a.block_number
        ,a.tx_hash
        ,a.evt_index
        ,a.sub_type
        ,a.sub_idx
        ,a.offer_first_item_type
        ,a.consideration_first_item_type
        ,a.sender
        ,case when b.tx_hash is NOT NULL then b.receiver
              else a.receiver
          end AS receiver
        ,a.zone
        ,a.token_contract_address
        ,a.original_amount
        ,a.item_type
        ,a.token_id
        ,a.platform_contract_address
        ,a.offer_cnt
        ,a.consideration_cnt
        ,a.is_private
        ,a.eth_erc_idx
        ,a.nft_cnt
        ,a.erc721_cnt
        ,a.erc1155_cnt
        ,a.order_type
        ,a.is_price
        ,a.is_netprice
        ,a.is_platform_fee
        ,a.is_creator_fee
        ,a.creator_fee_idx
        ,a.is_traded_nft
        ,a.is_moved_nft
  FROM ref_seaport_ethereum_base_pairs a
  LEFT JOIN ref_seaport_ethereum_base_pairs b on b.tx_hash = a.tx_hash
    AND b.evt_index = a.evt_index
    AND b.block_time = a.block_time -- for performance
    AND b.token_contract_address = a.token_contract_address
    AND b.token_id = a.token_id
    AND b.original_amount = a.original_amount
    AND b.is_moved_nft
  where 1=1
    AND a.is_private
    AND NOT a.is_moved_nft
    AND a.consideration_cnt > 0
)
,iv_volume AS (
  SELECT block_time
        ,tx_hash
        ,evt_index
        ,max(token_contract_address) AS token_contract_address
        ,sum(case when is_price then original_amount end) AS price_amount_raw
        ,sum(case when is_platform_fee then original_amount end) AS platform_fee_amount_raw
        ,max(case when is_platform_fee then receiver end) AS platform_fee_receiver
        ,sum(case when is_creator_fee then original_amount end) AS creator_fee_amount_raw
        ,sum(case when is_creator_fee AND creator_fee_idx = 1 then original_amount end) AS creator_fee_amount_raw_1
        ,sum(case when is_creator_fee AND creator_fee_idx = 2 then original_amount end) AS creator_fee_amount_raw_2
        ,sum(case when is_creator_fee AND creator_fee_idx = 3 then original_amount end) AS creator_fee_amount_raw_3
        ,sum(case when is_creator_fee AND creator_fee_idx = 4 then original_amount end) AS creator_fee_amount_raw_4
        ,max(case when is_creator_fee AND creator_fee_idx = 1 then receiver end) AS creator_fee_receiver_1
        ,max(case when is_creator_fee AND creator_fee_idx = 2 then receiver end) AS creator_fee_receiver_2
        ,max(case when is_creator_fee AND creator_fee_idx = 3 then receiver end) AS creator_fee_receiver_3
        ,max(case when is_creator_fee AND creator_fee_idx = 4 then receiver end) AS creator_fee_receiver_4
  FROM iv_base_pairs_priv a
  where 1=1
    AND eth_erc_idx > 0
  group by 1,2,3
)
,iv_nfts AS (
  SELECT a.block_time
        ,a.tx_hash
        ,a.evt_index
        ,a.block_number
        ,a.sender AS seller
        ,a.receiver AS buyer
        ,case when nft_cnt > 1 then 'bundle trade'
              else 'single item trade'
          end AS trade_type
        ,a.order_type
        ,a.token_contract_address AS nft_contract_address
        ,a.original_amount AS nft_token_amount
        ,a.token_id AS nft_token_id
        ,a.item_type AS nft_token_standard
        ,a.zone
        ,a.platform_contract_address
        ,b.token_contract_address
        ,round(price_amount_raw / nft_cnt) AS price_amount_raw  -- to truncate the odd number of decimal places
        ,round(platform_fee_amount_raw / nft_cnt) AS platform_fee_amount_raw
        ,platform_fee_receiver
        ,round(creator_fee_amount_raw / nft_cnt) AS creator_fee_amount_raw
        ,creator_fee_amount_raw_1 / nft_cnt AS creator_fee_amount_raw_1
        ,creator_fee_amount_raw_2 / nft_cnt AS creator_fee_amount_raw_2
        ,creator_fee_amount_raw_3 / nft_cnt AS creator_fee_amount_raw_3
        ,creator_fee_amount_raw_4 / nft_cnt AS creator_fee_amount_raw_4
        ,creator_fee_receiver_1
        ,creator_fee_receiver_2
        ,creator_fee_receiver_3
        ,creator_fee_receiver_4
        ,case when nft_cnt > 1 then true
              else false
          end AS estimated_price
        ,is_private
        ,sub_idx
  FROM iv_base_pairs_priv a
  LEFT JOIN iv_volume b on b.block_time = a.block_time  -- tx_hash AND evt_index is PK, but for performance, block_time is included
    AND b.tx_hash = a.tx_hash
    AND b.evt_index = a.evt_index
  where 1=1
    AND a.is_traded_nft
)
,iv_trades AS (
  SELECT a.*
          ,try_cast(date_trunc('day', a.block_time) AS date) AS block_date
          ,n.name AS nft_token_name
          ,t.`FROM` AS tx_from
          ,t.`to` AS tx_to
          ,right(t.data,8) AS right_hash
          ,case when a.token_contract_address = '{{c_native_token_address}}' then '{{c_native_symbol}}'
                else e.symbol
           end AS token_symbol
          ,case when a.token_contract_address = '{{c_native_token_address}}' then '{{c_alternative_token_address}}'
                else a.token_contract_address
           end AS token_alternative_symbol
          ,e.decimals AS price_token_decimals
          ,a.price_amount_raw / power(10, e.decimals) AS price_amount
          ,a.price_amount_raw / power(10, e.decimals) * p.price AS price_amount_usd
          ,a.platform_fee_amount_raw / power(10, e.decimals) AS platform_fee_amount
          ,a.platform_fee_amount_raw / power(10, e.decimals) * p.price AS platform_fee_amount_usd
          ,a.creator_fee_amount_raw / power(10, e.decimals) AS creator_fee_amount
          ,a.creator_fee_amount_raw / power(10, e.decimals) * p.price AS creator_fee_amount_usd
          ,case when right(t.data,8) = '72db8c0b' then 'Gem'
                when right(t.data,8) = '332d1229' THEN 'Blur'
                else agg.name
           end AS aggregator_name
          ,agg.contract_address AS aggregator_address
          ,'seaport-' || tx_hash || '-' || evt_index || '-' || nft_contract_address || '-' || nft_token_id || '-' || sub_idx AS unique_trade_id
  FROM iv_nfts a
  inner join source_ethereum_transactions t on t.hash = a.tx_hash
  LEFT JOIN ref_tokens_nft n on n.contract_address = nft_contract_address
  LEFT JOIN ref_tokens_erc20 e on e.contract_address = case when a.token_contract_address = '{{c_native_token_address}}' then '{{c_alternative_token_address}}'
                                                            else a.token_contract_address
                                                      end
  LEFT JOIN source_prices_usd p on p.contract_address = case when a.token_contract_address = '{{c_native_token_address}}' then '{{c_alternative_token_address}}'
                                                            else a.token_contract_address
                                                        end
    AND p.minute = date_trunc('minute', a.block_time)
  LEFT JOIN ref_nft_aggregators agg on agg.contract_address = t.to
)
,iv_columns AS (
  -- Rename column to align other *.trades tables
  -- But the columns ordering is according to convenience.
  -- initcap the code value if needed
  SELECT
    -- basic info
    'ethereum' AS blockchain
    ,'seaport' AS project
    ,'v1' AS version

    -- order info
    ,block_date
    ,block_time
    ,seller
    ,buyer
    ,initcap(trade_type) AS trade_type
    ,initcap(order_type) AS trade_category -- Buy / Offer Accepted
    ,'Trade' AS evt_type

    -- nft token info
    ,nft_contract_address
    ,nft_token_name AS collection
    ,nft_token_id AS token_id
    ,nft_token_amount AS number_of_items
    ,nft_token_standard AS token_standard

    -- price info
    ,price_amount AS amount_original
    ,price_amount_raw AS amount_raw
    ,price_amount_usd AS amount_usd
    ,token_symbol AS currency_symbol
    ,token_alternative_symbol AS currency_contract
    ,token_contract_address AS original_currency_contract
    ,price_token_decimals AS currency_decimals   -- in case calculating royalty1~4

    -- project info (platform or exchange)
    ,platform_contract_address AS project_contract_address
    ,platform_fee_receiver AS platform_fee_receive_address
    ,platform_fee_amount_raw
    ,platform_fee_amount
    ,platform_fee_amount_usd

    -- royalty info
    ,creator_fee_receiver_1 AS royalty_fee_receive_address
    ,creator_fee_amount_raw AS royalty_fee_amount_raw
    ,creator_fee_amount AS royalty_fee_amount
    ,creator_fee_amount_usd AS royalty_fee_amount_usd
    ,creator_fee_receiver_1 AS royalty_fee_receive_address_1
    ,creator_fee_receiver_2 AS royalty_fee_receive_address_2
    ,creator_fee_receiver_3 AS royalty_fee_receive_address_3
    ,creator_fee_receiver_4 AS royalty_fee_receive_address_4
    ,creator_fee_amount_raw_1 AS royalty_fee_amount_raw_1
    ,creator_fee_amount_raw_2 AS royalty_fee_amount_raw_2
    ,creator_fee_amount_raw_3 AS royalty_fee_amount_raw_3
    ,creator_fee_amount_raw_4 AS royalty_fee_amount_raw_4

    -- aggregator
    ,aggregator_name
    ,aggregator_address

    -- tx
    ,block_number
    ,tx_hash
    ,evt_index
    ,tx_from
    ,tx_to
    ,right_hash

    -- seaport etc
    ,zone AS zone_address
    ,estimated_price
    ,is_private

    -- unique key
    ,unique_trade_id
  FROM iv_trades
)
SELECT *
FROM iv_columns
;