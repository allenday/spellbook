{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index', 'nft_contract_address', 'token_id'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                            "project",
                            "seaport",
                            \'["sohwak"]\') }}'
    )
}}

{% set c_native_token_address = "0x0000000000000000000000000000000000000000" %}
{% set c_alternative_token_address = "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7" %}  -- WAVAX
{% set c_native_symbol = "AVAX" %}
{% set c_seaport_first_date = "2022-06-01" %}

with source_avalanche_c_transactions AS (
    SELECT *
    FROM {{ source('avalanche_c', 'transactions') }}
    {% if NOT is_incremental() %}
    where block_time >= date '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
    where block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,ref_seaport_avalanche_c_base_pairs AS (
      SELECT *
      FROM {{ ref('seaport_avalanche_c_base_pairs') }}
      where 1=1
      {% if is_incremental() %}
            AND block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}
)
,ref_tokens_nft AS (
    SELECT *
    FROM {{ ref('tokens_nft') }}
    where blockchain = 'avalanche_c'
)
,ref_tokens_erc20 AS (
    SELECT *
    FROM {{ ref('tokens_erc20') }}
    where blockchain = 'avalanche_c'
)
,ref_nft_aggregators AS (
    SELECT *
    FROM {{ ref('nft_aggregators') }}
    where blockchain = 'avalanche_c'
)
,source_prices_usd AS (
    SELECT *
    FROM {{ source('prices', 'usd') }}
    where blockchain = 'avalanche_c'
    {% if NOT is_incremental() %}
      AND minute >= date '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
      AND minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,iv_base_pairs_priv AS (
  SELECT a.block_date
        ,a.block_time
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
  FROM ref_seaport_avalanche_c_base_pairs a
  where 1=1
    AND NOT a.is_private
  union all
  SELECT a.block_date
        ,a.block_time
        ,a.block_number
        ,a.tx_hash
        ,a.evt_index
        ,a.sub_type
        ,a.sub_idx
        ,a.offer_first_item_type
        ,a.consideration_first_item_type
        ,a.sender
        ,case WHEN b.tx_hash is NOT NULL THEN b.receiver
              ELSE a.receiver
          END AS receiver
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
  FROM ref_seaport_avalanche_c_base_pairs a
  LEFT JOIN ref_seaport_avalanche_c_base_pairs b ON b.tx_hash = a.tx_hash
    AND b.evt_index = a.evt_index
    AND b.block_date = a.block_date -- for performance
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
  SELECT block_date
        ,block_time
        ,tx_hash
        ,evt_index
        ,max(token_contract_address) AS token_contract_address
        ,sum(case WHEN is_price THEN original_amount END) AS price_amount_raw
        ,sum(case WHEN is_platform_fee THEN original_amount END) AS platform_fee_amount_raw
        ,max(case WHEN is_platform_fee THEN receiver END) AS platform_fee_receiver
        ,sum(case WHEN is_creator_fee THEN original_amount END) AS creator_fee_amount_raw
        ,sum(case WHEN is_creator_fee AND creator_fee_idx = 1 THEN original_amount END) AS creator_fee_amount_raw_1
        ,sum(case WHEN is_creator_fee AND creator_fee_idx = 2 THEN original_amount END) AS creator_fee_amount_raw_2
        ,sum(case WHEN is_creator_fee AND creator_fee_idx = 3 THEN original_amount END) AS creator_fee_amount_raw_3
        ,sum(case WHEN is_creator_fee AND creator_fee_idx = 4 THEN original_amount END) AS creator_fee_amount_raw_4
        ,max(case WHEN is_creator_fee AND creator_fee_idx = 1 THEN receiver END) AS creator_fee_receiver_1
        ,max(case WHEN is_creator_fee AND creator_fee_idx = 2 THEN receiver END) AS creator_fee_receiver_2
        ,max(case WHEN is_creator_fee AND creator_fee_idx = 3 THEN receiver END) AS creator_fee_receiver_3
        ,max(case WHEN is_creator_fee AND creator_fee_idx = 4 THEN receiver END) AS creator_fee_receiver_4
  FROM iv_base_pairs_priv a
  where 1=1
    AND eth_erc_idx > 0
  GROUP BY 1,2,3,4
)
,iv_nfts AS (
  SELECT a.block_date
        ,a.block_time
        ,a.tx_hash
        ,a.evt_index
        ,a.block_number
        ,a.sender AS seller
        ,a.receiver AS buyer
        ,case WHEN nft_cnt > 1 THEN 'bundle trade'
              ELSE 'single item trade'
          END AS trade_type
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
        ,case WHEN nft_cnt > 1 THEN true
              ELSE false
          END AS estimated_price
        ,is_private
        ,sub_type
        ,sub_idx
  FROM iv_base_pairs_priv a
  LEFT JOIN iv_volume b ON b.block_date = a.block_date  -- tx_hash AND evt_index is PK, but for performance, block_time is included
    AND b.tx_hash = a.tx_hash
    AND b.evt_index = a.evt_index
  where 1=1
    AND a.is_traded_nft
)
,iv_trades AS (
  SELECT a.*
          ,n.name AS nft_token_name
          ,t.`FROM` AS tx_from
          ,t.`to` AS tx_to
          ,right(t.data,8) AS right_hash
          ,case WHEN a.token_contract_address = '{{c_native_token_address}}' THEN '{{c_native_symbol}}'
                ELSE e.symbol
           END AS token_symbol
          ,case WHEN a.token_contract_address = '{{c_native_token_address}}' THEN '{{c_alternative_token_address}}'
                ELSE a.token_contract_address
           END AS token_alternative_symbol
          ,e.decimals AS price_token_decimals
          ,a.price_amount_raw / power(10, e.decimals) AS price_amount
          ,a.price_amount_raw / power(10, e.decimals) * p.price AS price_amount_usd
          ,a.platform_fee_amount_raw / power(10, e.decimals) AS platform_fee_amount
          ,a.platform_fee_amount_raw / power(10, e.decimals) * p.price AS platform_fee_amount_usd
          ,a.creator_fee_amount_raw / power(10, e.decimals) AS creator_fee_amount
          ,a.creator_fee_amount_raw / power(10, e.decimals) * p.price AS creator_fee_amount_usd
          ,agg.name AS aggregator_name
          ,agg.contract_address AS aggregator_address
          ,sub_idx
  FROM iv_nfts a
  inner join source_avalanche_c_transactions t ON t.hash = a.tx_hash
  LEFT JOIN ref_tokens_nft n ON n.contract_address = nft_contract_address
  LEFT JOIN ref_tokens_erc20 e ON e.contract_address = case WHEN a.token_contract_address = '{{c_native_token_address}}' THEN '{{c_alternative_token_address}}'
                                                            ELSE a.token_contract_address
                                                      END
  LEFT JOIN source_prices_usd p ON p.contract_address = case WHEN a.token_contract_address = '{{c_native_token_address}}' THEN '{{c_alternative_token_address}}'
                                                            ELSE a.token_contract_address
                                                        END
    AND p.minute = date_trunc('minute', a.block_time)
  LEFT JOIN ref_nft_aggregators agg ON agg.contract_address = t.to
)
,iv_columns AS (
  -- Rename column to align other *.trades tables
  -- But the columns ordering is according to convenience.
  -- initcap the code value if needed
  SELECT
    -- basic info
    'avalanche_c' AS blockchain
    ,'seaport' AS project
    ,'v1' AS version

    -- ORDER info
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
    ,sub_idx
    ,sub_type
  FROM iv_trades
)
SELECT *
FROM iv_columns
;