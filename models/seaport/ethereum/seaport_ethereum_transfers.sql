-- ## this function consists of 4 parts,
-- ## p1_ : seaport."Seaport_call_fulfillBasicOrder"
-- ## p2_ : seaport."Seaport_call_fulfillAvailableAdvancedOrders"
-- ##       seaport."Seaport_call_fulfillAvailableOrders"
-- ## p3_ : seaport."Seaport_call_fulfillOrder"
-- ##       seaport."Seaport_call_fulfillAdvancedOrder"
-- ## p4_ : seaport."Seaport_call_matchOrders"
-- ##     : seaport."Seaport_call_matchAdvancedOrders"

{{ config(
    alias = 'transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "seaport",
                            \'["sohawk", "soispoke"]\') }}'
    )
}}

with p1_call AS (
    SELECT 'basic_order' AS main_type
          ,call_tx_hash AS tx_hash
          ,call_block_time AS block_time
          ,call_block_number AS block_number
          ,max(get_json_object(parameters, "$.basicOrderType")) AS order_type_id
      FROM {{ source('seaport_ethereum', 'Seaport_call_fulfillBasicOrder') }}
      {% if is_incremental() %} -- this filter will only be applied ON an incremental run
      where call_block_time >= (SELECT max(block_time) FROM {{ this }})
      {% endif %}
     GROUP BY 1,2,3,4
)

,p1_evt AS (
    SELECT c.main_type
          ,c.tx_hash
          ,c.block_time
          ,c.block_number
          ,c.order_type_id
          ,'offer' AS sub_type
          ,offer_idx AS sub_idx
          ,e.offerer AS sender
          ,e.recipient AS receiver
          ,e.zone
          ,concat('0x',substr(get_json_object(offer2, "$.token"),3,40)) AS token_contract_address
          ,get_json_object(offer2, "$.amount") AS original_amount
          ,get_json_object(offer2, "$.itemType") AS item_type
          ,get_json_object(offer2, "$.identifier") AS token_id
          ,e.contract_address AS exchange_contract_address
          ,e.evt_index
      FROM
          (SELECT *, posexplode(offer) AS (offer_idx, offer2) FROM {{ source('seaport_ethereum', 'Seaport_evt_OrderFulfilled') }}
               {% if is_incremental() %} -- this filter will only be applied ON an incremental run
               where evt_block_time >= (SELECT max(block_time) FROM {{ this }})
               {% endif %}
            ) e
          inner join p1_call c ON c.tx_hash = e.evt_tx_hash
                      union all
    SELECT c.main_type
          ,c.tx_hash
          ,c.block_time
          ,c.block_number
          ,c.order_type_id
          ,'consideration' AS sub_type
          ,consideration_idx AS sub_idx
          ,e.recipient AS sender
          ,concat('0x',substr(get_json_object(consideration2, "$.recipient"),3,40)) AS receiver
          ,e.zone
          ,concat('0x',substr(get_json_object(consideration2, "$.token"),3,40)) AS token_contract_address
          ,get_json_object(consideration2, "$.amount") AS original_amount
          ,get_json_object(consideration2, "$.itemType") AS item_type
          ,get_json_object(consideration2, "$.identifier") AS token_id
          ,e.contract_address AS exchange_contract_address
          ,e.evt_index
      FROM
        (SELECT *, posexplode(consideration) AS (consideration_idx, consideration2) FROM {{ source('seaport_ethereum', 'Seaport_evt_OrderFulfilled') }}
            {% if is_incremental() %} -- this filter will only be applied ON an incremental run
            where evt_block_time >= (SELECT max(block_time) FROM {{ this }})
            {% endif %}
            ) e
        inner join p1_call c ON c.tx_hash = e.evt_tx_hash
     )


,p1_add_rn AS (SELECT (max(case WHEN purchase_method = 'Offer Accepted' AND sub_type = 'offer' AND sub_idx = 0 THEN token_contract_address
                     WHEN purchase_method = 'Buy' AND sub_type = 'consideration' THEN token_contract_address
                END) over (partition BY tx_hash, evt_index)) AS avg_original_currency_contract
          ,sum(case WHEN purchase_method = 'Offer Accepted' AND sub_type = 'offer' AND sub_idx = 0 THEN original_amount
                    WHEN purchase_method = 'Buy' AND sub_type = 'consideration' THEN original_amount
               END) over (partition BY tx_hash, evt_index)
 / nft_transfer_count AS avg_original_amount
          ,sum(case WHEN fee_royalty_yn = 'fee' THEN original_amount END) over (partition BY tx_hash, evt_index) / nft_transfer_count AS avg_fee_amount
          ,sum(case WHEN fee_royalty_yn = 'royalty' THEN original_amount END) over (partition BY tx_hash, evt_index) / nft_transfer_count AS avg_royalty_amount
          , (max(case WHEN fee_royalty_yn = 'fee' THEN receiver END) over (partition BY tx_hash, evt_index)) AS avg_fee_receive_address
          , (max(case WHEN fee_royalty_yn = 'royalty' THEN receiver END) over (partition BY tx_hash, evt_index)) AS avg_royalty_receive_address
          ,a.*
      FROM (SELECT case WHEN purchase_method = 'Offer Accepted' AND sub_type = 'consideration' AND fee_royalty_idx = 1 THEN 'fee'
                        WHEN purchase_method = 'Offer Accepted' AND sub_type = 'consideration' AND fee_royalty_idx = 2 THEN 'royalty'
                        WHEN purchase_method = 'Buy' AND sub_type = 'consideration' AND fee_royalty_idx = 2 THEN 'fee'
                        WHEN purchase_method = 'Buy' AND sub_type = 'consideration' AND fee_royalty_idx = 3 THEN 'royalty'
                   END AS fee_royalty_yn
                  ,case WHEN purchase_method = 'Offer Accepted' AND main_type = 'order' THEN 'Individual Offer'
                        WHEN purchase_method = 'Offer Accepted' AND main_type = 'basic_order' THEN 'Individual Offer'
                        WHEN purchase_method = 'Offer Accepted' AND main_type = 'advanced_order' THEN 'Collection / Trait Offers'
                        ELSE 'Buy'
                   END AS order_type
                  ,a.*
              FROM (SELECT (count(case WHEN item_type in ('2', '3') THEN 1 END) over (partition BY tx_hash, evt_index)) AS nft_transfer_count
                          , (sum(case WHEN item_type in ('0', '1') THEN 1 END) over (partition BY tx_hash, evt_index, sub_type order BY sub_idx)) AS fee_royalty_idx
                          ,case WHEN max(case WHEN (sub_type,sub_idx,item_type) in (('offer',0,'1')) THEN 1 ELSE 0 END) over (partition BY tx_hash) = 1 THEN 'Offer Accepted'
                                ELSE 'Buy'
                           END AS purchase_method
                          ,a.*
                      FROM p1_evt a
                    ) a
            ) a
)

,p1_txn_level AS (
    SELECT main_type
          ,sub_idx
          ,tx_hash
          ,block_time
          ,block_number
          ,zone
          ,exchange_contract_address
          ,evt_index
          ,order_type
          ,purchase_method
          ,receiver AS buyer
          ,sender AS seller
          ,avg_original_amount AS original_amount
          ,avg_original_currency_contract AS original_currency_contract
          ,avg_fee_receive_address AS fee_receive_address
          ,avg_fee_amount AS fee_amount
          ,avg_original_currency_contract AS fee_currency_contract
          ,avg_royalty_receive_address AS royalty_receive_address
          ,avg_royalty_amount AS royalty_amount
          ,avg_original_currency_contract AS royalty_currency_contract
          ,token_contract_address AS nft_contract_address
          ,token_id AS nft_token_id
          ,nft_transfer_count
          ,original_amount AS nft_item_count
          ,coalesce(avg_original_amount,0) + coalesce(avg_fee_amount,0) + coalesce(avg_royalty_amount,0) AS attempt_amount
          ,0 AS revert_amount
          ,false AS reverted
          ,case WHEN nft_transfer_count > 1 THEN true ELSE false END AS price_estimated
          ,'' AS offer_order_type
          ,item_type
          ,order_type_id
      FROM p1_add_rn a
     where item_type in ('2', '3')
)




,p1_seaport_transfers AS (SELECT
          'ethereum' AS blockchain
          ,'seaport' AS project
          ,'v1' AS version
          ,TRY_CAST(date_trunc('DAY', a.block_time) AS date) AS block_date
          ,a.block_time
          ,a.block_number
          ,a.nft_token_id AS token_id
          ,n.name AS collection
          ,a.attempt_amount / power(10,t1.decimals) * p1.price AS amount_usd
          ,case WHEN item_type = '2' THEN 'erc721'
                WHEN item_type = '3' THEN 'erc1155'
           END AS token_standard
          ,case WHEN order_type = 'Bulk Purchase' THEN 'Bulk Purchase'
                WHEN nft_transfer_count = 1 THEN 'Single Item Trade'
                ELSE 'Bundle Trade'
           END AS trade_type
          ,nft_item_count AS number_of_items
          ,a.purchase_method AS trade_category
          ,'Trade' AS evt_type
          ,concat('0x',substr(seller,3,40)) AS seller
          , CASE WHEN concat('0x',substr(buyer,3,40))=agg.contract_address THEN COALESCE(erct2.to, erct3.to)
            ELSE concat('0x',substr(buyer,3,40)) END AS buyer
          ,a.original_amount / power(10,t1.decimals) AS amount_original
          ,a.original_amount AS amount_raw
          ,case WHEN a.original_currency_contract = '0x0000000000000000000000000000000000000000' THEN 'ETH'
                ELSE t1.symbol
           END AS currency_symbol
          ,case WHEN a.original_currency_contract = '0x0000000000000000000000000000000000000000' THEN
          '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE a.original_currency_contract
           END AS currency_contract
          ,nft_contract_address
          ,a.exchange_contract_address AS project_contract_address
          ,coalesce(agg_m.aggregator_name, agg.name) AS aggregator_name
          ,agg.contract_address AS aggregator_address
          ,a.tx_hash
          ,tx.from AS tx_from
          ,tx.to AS tx_to
          ,ROUND((2.5*(a.original_amount) / 100),7) AS platform_fee_amount_raw
          ,ROUND((2.5*((a.original_amount / power(10,t1.decimals)))/100),7) AS platform_fee_amount
          ,ROUND((2.5*((a.original_amount / power(10,t1.decimals)* p1.price))/100),7) AS platform_fee_amount_usd
          ,'2.5' AS platform_fee_percentage
          ,a.royalty_amount AS royalty_fee_amount_raw
          ,a.royalty_amount / power(10,t1.decimals) AS royalty_fee_amount
          ,a.royalty_amount / power(10,t1.decimals) * p1.price AS royalty_fee_amount_usd
          , (a.royalty_amount / a.original_amount * 100)::STRING  AS royalty_fee_percentage
          ,a.royalty_receive_address AS royalty_fee_receive_address
          ,case WHEN royalty_amount > 0 AND a.original_currency_contract =
          '0x0000000000000000000000000000000000000000' THEN 'ETH'
                WHEN royalty_amount > 0 THEN t1.symbol
          END AS royalty_fee_currency_symbol
          ,a.tx_hash || '-' || a.nft_token_id || '-' || a.original_amount::STRING || '-' ||  concat('0x',substr(seller,3,40)) || '-' ||
          order_type_id::STRING || '-' || cast(row_number () over (partition BY a.tx_hash order BY sub_idx) AS
          STRING) AS unique_trade_id,
          a.zone
      FROM p1_txn_level a
        inner join {{ source('ethereum', 'transactions') }} tx
            ON tx.hash = a.tx_hash
            {% if NOT is_incremental() %}
            AND tx.block_number > 14801608
            {% endif %}
            {% if is_incremental() %}
            AND tx.block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN {{ ref('nft_ethereum_aggregators_markers') }} agg_m
                ON LEFT(tx.data, CHARINDEX(agg_m.hash_marker, tx.data) + LENGTH(agg_m.hash_marker)) LIKE '%' || agg_m.hash_marker
        LEFT JOIN {{ ref('nft_aggregators') }} agg
            ON agg.contract_address = tx.to AND agg.blockchain = 'ethereum'
        LEFT JOIN {{ ref('tokens_nft') }} n
            ON n.contract_address = nft_contract_address AND n.blockchain = 'ethereum'
        LEFT JOIN {{ source('erc721_ethereum', 'evt_transfer') }} erct2 ON erct2.evt_block_time=a.block_time
            AND nft_contract_address=erct2.contract_address
            AND erct2.evt_tx_hash=a.tx_hash
            AND erct2.tokenId=a.nft_token_id
            AND erct2.from=concat('0x',substr(buyer,3,40))
            {% if NOT is_incremental() %}
            AND erct2.evt_block_number > 14801608
            {% endif %}
            {% if is_incremental() %}
            AND erct2.evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN {{ source('erc1155_ethereum', 'evt_transfersingle') }} erct3 ON erct3.evt_block_time=a.block_time
            AND nft_contract_address=erct3.contract_address
            AND erct3.evt_tx_hash=a.tx_hash
            AND erct3.id=a.nft_token_id
            AND erct3.from=concat('0x',substr(buyer,3,40))
            {% if NOT is_incremental() %}
            AND erct3.evt_block_number > 14801608
            {% endif %}
            {% if is_incremental() %}
            AND erct3.evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN {{ ref('tokens_erc20') }} t1
            ON t1.contract_address =
                case WHEN a.original_currency_contract = '0x0000000000000000000000000000000000000000'
                THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE a.original_currency_contract
                END
            AND t1.blockchain = 'ethereum'
          LEFT JOIN {{ source('prices', 'usd') }} p1
            ON p1.contract_address =
                case WHEN a.original_currency_contract = '0x0000000000000000000000000000000000000000'
                THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE a.original_currency_contract
                END
            AND p1.minute = date_trunc('minute', a.block_time)
            AND p1.blockchain = 'ethereum'
            {% if is_incremental() %}
            AND p1.minute >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            )

,p2_call AS (
    SELECT 'available_advanced_orders' AS main_type
          ,'bulk' AS sub_type
          ,idx AS sub_idx
          ,get_json_object(get_json_object(each, "$.parameters"), "$.zone") AS zone
          ,get_json_object(get_json_object(each, "$.parameters"), "$.offerer") AS offerer
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.offer[0]"), "$.token") AS offer_token
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.offer[0]"), "$.itemType") AS offer_item_type
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.offer[0]"), "$.identifierOrCriteria") AS offer_identifier
          ,get_json_object(get_json_object(each, "$.parameters"), "$.orderType") AS offer_order_type
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[0]"), "$.token") AS price_token
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[0]"), "$.itemType") AS price_item_type
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[0]"), "$.startAmount") AS price_amount
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[1]"), "$.startAmount") AS fee_amount
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[2]"), "$.startAmount") AS royalty_amount
          ,c.call_tx_hash AS tx_hash
          ,c.call_block_time AS block_time
          ,c.call_block_number AS block_number
          ,c.contract_address AS exchange_contract_address
      FROM (SELECT *, posexplode(advancedOrders) AS (idx, each) from {{ source('seaport_ethereum', 'Seaport_call_fulfillAvailableAdvancedOrders') }}
      {% if is_incremental() %} -- this filter will only be applied ON an incremental run
      where call_block_time >= (SELECT max(block_time) FROM {{ this }})
      {% endif %}
      ) c
       where call_success

                                                  union all
      SELECT 'available_orders' AS main_type
          ,'bulk' AS sub_type
          ,idx AS sub_idx
          ,get_json_object(get_json_object(each, "$.parameters"), "$.zone") AS zone
          ,get_json_object(get_json_object(each, "$.parameters"), "$.offerer") AS offerer
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.offer[0]"), "$.token") AS offer_token
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.offer[0]"), "$.itemType") AS offer_item_type
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.offer[0]"), "$.identifierOrCriteria") AS offer_identifier
          ,get_json_object(get_json_object(each, "$.parameters"), "$.orderType") AS offer_order_type
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[0]"), "$.token") AS price_token
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[0]"), "$.itemType") AS price_item_type
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[0]"), "$.startAmount") AS price_amount
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[1]"), "$.startAmount") AS fee_amount
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[2]"), "$.startAmount") AS royalty_amount
          ,c.call_tx_hash AS tx_hash
          ,c.call_block_time AS block_time
          ,c.call_block_number AS block_number
          ,c.contract_address AS exchange_contract_address
      FROM (SELECT *, posexplode(orders) AS (idx, each) from {{ source('seaport_ethereum', 'Seaport_call_fulfillAvailableOrders') }}
      {% if is_incremental() %} -- this filter will only be applied ON an incremental run
      where call_block_time >= (SELECT max(block_time) FROM {{ this }})
      {% endif %}
      ) c
      where call_success
)
,p2_evt AS (
 SELECT c.*
          ,evt_tx_hash
          ,e.recipient
          ,get_json_object(offer[0], "$.amount") AS evt_token_amount
          ,get_json_object(consideration[0], "$.token") AS evt_price_token
          ,get_json_object(consideration[0], "$.amount") AS evt_price_amount
          ,get_json_object(consideration[0], "$.itemType") AS evt_price_item_type
          ,get_json_object(consideration[0], "$.recipient") AS evt_price_recipient
          ,get_json_object(consideration[0], "$.identifier") AS evt_price_identifier
          ,get_json_object(consideration[0], "$.token") AS evt_fee_token
          ,get_json_object(consideration[1], "$.amount") AS evt_fee_amount
          ,get_json_object(consideration[1], "$.itemType") AS evt_fee_item_type
          ,get_json_object(consideration[1], "$.recipient") AS evt_fee_recipient
          ,get_json_object(consideration[1], "$.identifier") AS evt_fee_identifier
          ,get_json_object(consideration[2], "$.token") AS evt_royalty_token
          ,get_json_object(consideration[2], "$.amount") AS evt_royalty_amount
          ,get_json_object(consideration[2], "$.itemType") AS evt_royalty_item_type
          ,get_json_object(consideration[2], "$.recipient") AS evt_royalty_recipient
          ,get_json_object(consideration[2], "$.identifier") AS evt_royalty_identifier
          ,e.evt_index
      FROM p2_call c
            inner join {{ source('seaport_ethereum', 'Seaport_evt_OrderFulfilled') }} e
            ON e.evt_tx_hash = c.tx_hash
            AND e.offerer = concat('0x',substr(c.offerer,3,40))
            AND get_json_object(e.offer[0], "$.token") = c.offer_token
            AND get_json_object(e.offer[0], "$.identifier") = c.offer_identifier
            AND get_json_object(e.offer[0], "$.itemType") = c.offer_item_type
)
,p2_transfer_level AS (
    SELECT a.main_type
          ,a.sub_idx
          ,a.tx_hash
          ,a.block_time
          ,a.block_number
          ,a.zone
          ,a.exchange_contract_address
          ,offer_token AS nft_address
          ,offer_identifier AS nft_token_id
          ,recipient AS buyer
          ,offerer AS seller
          ,offer_item_type AS offer_item_type
          ,offer_order_type AS offer_order_type
          ,offer_identifier AS nft_token_id_dcnt
          ,price_token AS price_token
          ,price_item_type AS price_item_type
          ,price_amount AS price_amount
          ,fee_amount AS fee_amount
          ,royalty_amount AS royalty_amount
          ,evt_token_amount AS evt_token_amount
          ,evt_price_amount AS evt_price_amount
          ,evt_fee_amount AS evt_fee_amount
          ,evt_royalty_amount AS evt_royalty_amount
          ,evt_fee_token AS evt_fee_token
          ,evt_royalty_token AS evt_royalty_token
          ,evt_fee_recipient AS evt_fee_recipient
          ,evt_royalty_recipient AS evt_royalty_recipient
          ,coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) AS attempt_amount
          ,case WHEN evt_tx_hash is NOT NULL THEN coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) END AS trade_amount
          ,case WHEN evt_tx_hash is NULL THEN coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) ELSE 0 END AS revert_amount
          ,case WHEN evt_tx_hash is NULL THEN true ELSE false END AS reverted
          ,'Bulk Purchase' AS trade_type
          ,'Bulk Purchase' AS order_type
          ,'Buy' AS purchase_method
      FROM p2_evt a
)

,p2_seaport_transfers AS (SELECT
          'ethereum' AS blockchain
          ,'seaport' AS project
          ,'v1' AS version
          ,TRY_CAST(date_trunc('DAY', a.block_time) AS date) AS block_date
          ,a.block_time
          ,a.block_number
          ,a.nft_token_id AS token_id
          ,n.name AS collection
          ,a.attempt_amount / power(10,t1.decimals) * p1.price AS amount_usd
          ,case WHEN offer_item_type = '2' THEN 'erc721'
                WHEN offer_item_type = '3' THEN 'erc1155'
           END AS token_standard
          ,trade_type
          ,evt_token_amount AS number_of_items
          ,a.purchase_method AS trade_category
          ,'Trade' AS evt_type
          ,concat('0x',substr(seller,3,40)) AS seller
          , CASE WHEN concat('0x',substr(buyer,3,40))=agg.contract_address THEN COALESCE(erct2.to, erct3.to)
            ELSE concat('0x',substr(buyer,3,40)) END AS buyer
          ,a.attempt_amount / power(10,t1.decimals) AS amount_original
          ,a.attempt_amount AS amount_raw
          ,case WHEN concat('0x',substr(a.price_token,3,40)) =
          '0x0000000000000000000000000000000000000000' THEN 'ETH'
                ELSE t1.symbol
           END AS currency_symbol
          ,case WHEN concat('0x',substr(a.price_token,3,40)) =
          '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE concat('0x',substr(a.price_token,3,40))
           END AS currency_contract
          ,concat('0x',substr(a.nft_address,3,40)) AS nft_contract_address
          ,a.exchange_contract_address AS project_contract_address
          ,coalesce(agg_m.aggregator_name, agg.name) AS aggregator_name
          ,agg.contract_address AS aggregator_address
          ,a.tx_hash
          ,tx.from AS tx_from
          ,tx.to AS tx_to
          ,ROUND((2.5*(a.attempt_amount) / 100),7) AS platform_fee_amount_raw
          ,ROUND((2.5*((a.attempt_amount / power(10,t1.decimals)))/100),7) AS platform_fee_amount
          ,ROUND((2.5*((a.attempt_amount / power(10,t1.decimals)* p1.price))/100),7) AS platform_fee_amount_usd
          ,'2.5' AS platform_fee_percentage
          ,a.evt_royalty_amount AS royalty_fee_amount_raw
          ,a.evt_royalty_amount / power(10,t1.decimals) AS royalty_fee_amount
          ,a.evt_royalty_amount / power(10,t1.decimals) * p1.price AS royalty_fee_amount_usd
          , (a.evt_royalty_amount / a.attempt_amount * 100)::STRING  AS royalty_fee_percentage
          ,case WHEN evt_royalty_amount > 0 THEN concat('0x',substr(evt_royalty_recipient,3,40)) END AS
          royalty_fee_receive_address
          ,case WHEN evt_royalty_amount > 0 AND concat('0x',substr(a.evt_royalty_token,3,40)) =
          '0x0000000000000000000000000000000000000000' THEN 'ETH'
                WHEN evt_royalty_amount > 0 THEN t1.symbol
          END AS royalty_fee_currency_symbol
          ,a.tx_hash || '-' || a.nft_token_id || '-' || a.attempt_amount::STRING || '-' ||  concat('0x',substr(seller,3,40)) || '-' ||
          cast(row_number () over (partition BY a.tx_hash order BY sub_idx) AS
          STRING) AS unique_trade_id,
          a.zone
      FROM p2_transfer_level a
        inner join {{ source('ethereum', 'transactions') }} tx
            ON tx.hash = a.tx_hash
            {% if NOT is_incremental() %}
            AND tx.block_number > 14801608
            {% endif %}
            {% if is_incremental() %}
            AND tx.block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN {{ source('erc721_ethereum', 'evt_transfer') }} erct2 ON erct2.evt_block_time=a.block_time
            AND concat('0x',substr(a.nft_address,3,40))=erct2.contract_address
            AND erct2.evt_tx_hash=a.tx_hash
            AND erct2.tokenId=a.nft_token_id
            AND erct2.from=concat('0x',substr(buyer,3,40))
            {% if NOT is_incremental() %}
            AND erct2.evt_block_number > 14801608
            {% endif %}
            {% if is_incremental() %}
            AND erct2.evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN {{ source('erc1155_ethereum', 'evt_transfersingle') }} erct3 ON erct3.evt_block_time=a.block_time
            AND concat('0x',substr(a.nft_address,3,40))=erct3.contract_address
            AND erct3.evt_tx_hash=a.tx_hash
            AND erct3.id=a.nft_token_id
            AND erct3.from=concat('0x',substr(buyer,3,40))
            {% if NOT is_incremental() %}
            AND erct3.evt_block_number > 14801608
            {% endif %}
            {% if is_incremental() %}
            AND erct3.evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN {{ ref('nft_ethereum_aggregators_markers') }} agg_m
                ON LEFT(tx.data, CHARINDEX(agg_m.hash_marker, tx.data) + LENGTH(agg_m.hash_marker)) LIKE '%' || agg_m.hash_marker
        LEFT JOIN {{ ref('nft_aggregators') }} agg
            ON agg.contract_address = tx.to AND agg.blockchain = 'ethereum'
        LEFT JOIN {{ ref('tokens_nft') }} n
            ON n.contract_address = concat('0x',substr(a.nft_address,3,40)) AND n.blockchain = 'ethereum'
        LEFT JOIN {{ ref('tokens_erc20') }} t1
            ON t1.contract_address =
                case WHEN concat('0x',substr(a.price_token,3,40)) = '0x0000000000000000000000000000000000000000'
                THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE concat('0x',substr(a.price_token,3,40))
                END AND t1.blockchain = 'ethereum'
          LEFT JOIN {{ source('prices', 'usd') }} p1
            ON p1.contract_address =
                case WHEN concat('0x',substr(a.price_token,3,40)) = '0x0000000000000000000000000000000000000000'
                THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE concat('0x',substr(a.price_token,3,40))
                END
            AND p1.minute = date_trunc('minute', a.block_time)
            AND p1.blockchain = 'ethereum'
            {% if is_incremental() %}
            AND p1.minute >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            )

,p3_call AS (SELECT 'order' AS main_type
          ,call_tx_hash AS tx_hash
          ,call_block_time AS block_time
          ,call_block_number AS block_number
          ,max(get_json_object(get_json_object(order, "$.parameters"), "$.orderType")) AS order_type_id
      FROM {{ source('seaport_ethereum', 'Seaport_call_fulfillOrder') }}
      {% if is_incremental() %} -- this filter will only be applied ON an incremental run
      where call_block_time >= (SELECT max(block_time) FROM {{ this }})
      {% endif %}
     GROUP BY 1,2,3,4
     union all
    SELECT 'advanced_order' AS main_type
          ,call_tx_hash AS tx_hash
          ,call_block_time AS block_time
          ,call_block_number AS block_number
          ,max(get_json_object(get_json_object(advancedOrder, "$.parameters"), "$.orderType")) AS order_type_id
      FROM {{ source('seaport_ethereum', 'Seaport_call_fulfillAdvancedOrder') }}
      {% if is_incremental() %} -- this filter will only be applied ON an incremental run
      where call_block_time >= (SELECT max(block_time) FROM {{ this }})
      {% endif %}
      GROUP BY 1,2,3,4)

,p3_evt AS (SELECT c.main_type
            ,c.tx_hash
            ,c.block_time
            ,c.block_number
            ,c.order_type_id
            ,'offer' AS sub_type
            ,offer_idx AS sub_idx
            ,e.offerer AS sender
            ,e.recipient AS receiver
            ,e.zone
            ,concat('0x',substr(get_json_object(offer2, "$.token"),3,40)) AS token_contract_address
            ,get_json_object(offer2, "$.amount") AS original_amount
            ,get_json_object(offer2, "$.itemType") AS item_type
            ,get_json_object(offer2, "$.identifier") AS token_id
            ,e.contract_address AS exchange_contract_address
            ,e.evt_index
        FROM
        (SELECT *, posexplode(offer) AS (offer_idx, offer2) FROM {{ source('seaport_ethereum', 'Seaport_evt_OrderFulfilled') }}
        {% if is_incremental() %} -- this filter will only be applied ON an incremental run
        where evt_block_time >= (SELECT max(block_time) FROM {{ this }})
        {% endif %}
        ) e
        inner join p3_call c ON c.tx_hash = e.evt_tx_hash
        union all
        SELECT c.main_type
            ,c.tx_hash
            ,c.block_time
            ,c.block_number
            ,c.order_type_id
            ,'consideration' AS sub_type
            ,consideration_idx AS sub_idx
            ,e.recipient AS sender
            ,concat('0x',substr(get_json_object(consideration2, "$.recipient"),3,40)) AS receiver
            ,e.zone
            ,concat('0x',substr(get_json_object(consideration2, "$.token"),3,40)) AS token_contract_address
            ,get_json_object(consideration2, "$.amount") AS original_amount
            ,get_json_object(consideration2, "$.itemType") AS item_type
            ,get_json_object(consideration2, "$.identifier") AS token_id
            ,e.contract_address AS exchange_contract_address
            ,e.evt_index
        FROM
        (SELECT *, posexplode(consideration) AS (consideration_idx, consideration2) FROM {{ source('seaport_ethereum', 'Seaport_evt_OrderFulfilled') }}
          {% if is_incremental() %} -- this filter will only be applied ON an incremental run
          where evt_block_time >= (SELECT max(block_time) FROM {{ this }})
          {% endif %}
          ) e
        inner join p3_call c ON c.tx_hash = e.evt_tx_hash
        )


,p3_add_rn AS (SELECT (max(case WHEN purchase_method = 'Offer Accepted' AND sub_type = 'offer' AND sub_idx = 0 THEN token_contract_address::STRING
                     WHEN purchase_method = 'Buy' AND sub_type = 'consideration' THEN token_contract_address::STRING
                END) over (partition BY tx_hash, evt_index)) AS avg_original_currency_contract
          ,sum(case WHEN purchase_method = 'Offer Accepted' AND sub_type = 'offer' AND sub_idx = 0 THEN original_amount
                    WHEN purchase_method = 'Buy' AND sub_type = 'consideration' THEN original_amount
               END) over (partition BY tx_hash, evt_index)
 / nft_transfer_count AS avg_original_amount
          ,sum(case WHEN fee_royalty_yn = 'fee' THEN original_amount END) over (partition BY tx_hash, evt_index) / nft_transfer_count AS avg_fee_amount
          ,sum(case WHEN fee_royalty_yn = 'royalty' THEN original_amount END) over (partition BY tx_hash, evt_index) / nft_transfer_count AS avg_royalty_amount
          , (max(case WHEN fee_royalty_yn = 'fee' THEN receiver::STRING END) over (partition BY tx_hash, evt_index)) AS avg_fee_receive_address
          , (max(case WHEN fee_royalty_yn = 'royalty' THEN receiver::STRING END) over (partition BY tx_hash, evt_index)) AS avg_royalty_receive_address
          ,a.*
      FROM (SELECT case WHEN purchase_method = 'Offer Accepted' AND sub_type = 'consideration' AND fee_royalty_idx = 1 THEN 'fee'
                        WHEN purchase_method = 'Offer Accepted' AND sub_type = 'consideration' AND fee_royalty_idx = 2 THEN 'royalty'
                        WHEN purchase_method = 'Buy' AND sub_type = 'consideration' AND fee_royalty_idx = 2 THEN 'fee'
                        WHEN purchase_method = 'Buy' AND sub_type = 'consideration' AND fee_royalty_idx = 3 THEN 'royalty'
                   END AS fee_royalty_yn
                  ,case WHEN purchase_method = 'Offer Accepted' AND main_type = 'order' THEN 'Individual Offer'
                        WHEN purchase_method = 'Offer Accepted' AND main_type = 'basic_order' THEN 'Individual Offer'
                        WHEN purchase_method = 'Offer Accepted' AND main_type = 'advanced_order' THEN 'Collection / Trait Offers'
                        ELSE 'Buy'
                   END AS order_type
                  ,a.*
              FROM (SELECT count(case WHEN item_type in ('2', '3') THEN 1 END) over (partition BY tx_hash, evt_index) AS nft_transfer_count
                          ,sum(case WHEN item_type in ('0', '1') THEN 1 END) over (partition BY tx_hash, evt_index, sub_type order BY sub_idx) AS fee_royalty_idx
                          ,case WHEN max(case WHEN (sub_type,sub_idx,item_type) in (('offer',0,'1')) THEN 1 ELSE 0 END) over (partition BY tx_hash) = 1 THEN 'Offer Accepted'
                                ELSE 'Buy'
                           END AS purchase_method
                          ,a.*
                      FROM p3_evt a
                    ) a
            ) a
)

,p3_txn_level AS (
    SELECT main_type
          ,sub_idx
          ,tx_hash
          ,block_time
          ,block_number
          ,zone
          ,exchange_contract_address
          ,evt_index
          ,order_type
          ,purchase_method
          ,receiver AS buyer
          ,sender AS seller
          ,avg_original_amount AS original_amount
          ,avg_original_currency_contract AS original_currency_contract
          ,avg_fee_receive_address AS fee_receive_address
          ,avg_fee_amount AS fee_amount
          ,avg_original_currency_contract AS fee_currency_contract
          ,avg_royalty_receive_address AS royalty_receive_address
          ,avg_royalty_amount AS royalty_amount
          ,avg_original_currency_contract AS royalty_currency_contract
          ,token_contract_address AS nft_contract_address
          ,token_id AS nft_token_id
          ,nft_transfer_count
          ,original_amount AS nft_item_count
--         quickfix for Issue #1510 that results in double counting of fees
--        ,coalesce(avg_original_amount,0) + coalesce(avg_fee_amount,0) + coalesce(avg_royalty_amount,0) AS attempt_amount
          ,coalesce(avg_original_amount,0) AS attempt_amount
          ,0 AS revert_amount
          ,false AS reverted
          ,case WHEN nft_transfer_count > 1 THEN true ELSE false END AS price_estimated
          ,'' AS offer_order_type
          ,item_type
          ,order_type_id
      FROM p3_add_rn a
     where item_type in ('2', '3')
)

,p3_seaport_transfers AS (SELECT
          'ethereum' AS blockchain
          ,'seaport' AS project
          ,'v1' AS version
          ,TRY_CAST(date_trunc('DAY', a.block_time) AS date) AS block_date
          ,a.block_time
          ,a.block_number
          ,a.nft_token_id AS token_id
          ,n.name AS collection
          ,a.attempt_amount / power(10,t1.decimals) * p1.price AS amount_usd
          ,case WHEN item_type = '2' THEN 'erc721'
                WHEN item_type = '3' THEN 'erc1155'
           END AS token_standard
          ,case WHEN order_type = 'Bulk Purchase' THEN 'Bulk Purchase'
                WHEN nft_transfer_count = 1 THEN 'Single Item Trade'
                ELSE 'Bundle Trade'
           END AS trade_type
          ,nft_transfer_count AS number_of_items
          ,a.purchase_method AS trade_category
          ,'Trade' AS evt_type
          ,concat('0x',substr(seller,3,40)) AS seller
          , CASE WHEN concat('0x',substr(buyer,3,40))=agg.contract_address THEN COALESCE(erct2.to, erct3.to)
            ELSE concat('0x',substr(buyer,3,40)) END AS buyer
          ,a.attempt_amount / power(10,t1.decimals) AS amount_original
          ,a.attempt_amount AS amount_raw
          ,case WHEN a.original_currency_contract = '0x0000000000000000000000000000000000000000' THEN 'ETH'
                ELSE t1.symbol
           END AS currency_symbol
          ,case WHEN a.original_currency_contract = '0x0000000000000000000000000000000000000000' THEN
          '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE a.original_currency_contract
           END AS currency_contract
          ,nft_contract_address
          ,a.exchange_contract_address AS project_contract_address
          ,coalesce(agg_m.aggregator_name, agg.name) AS aggregator_name
          ,agg.contract_address AS aggregator_address
          ,a.tx_hash
          ,tx.from AS tx_from
          ,tx.to AS tx_to
          ,ROUND((2.5*(a.attempt_amount) / 100),7) AS platform_fee_amount_raw
          ,ROUND((2.5*((a.attempt_amount / power(10,t1.decimals)))/100),7) AS platform_fee_amount
          ,ROUND((2.5*((a.attempt_amount / power(10,t1.decimals)* p1.price))/100),7) AS platform_fee_amount_usd
          ,'2.5' AS platform_fee_percentage
          ,a.royalty_amount AS royalty_fee_amount_raw
          ,a.royalty_amount / power(10,t1.decimals) AS royalty_fee_amount
          ,a.royalty_amount / power(10,t1.decimals) * p1.price AS royalty_fee_amount_usd
          , (a.royalty_amount / a.attempt_amount * 100)::STRING  AS royalty_fee_percentage
          ,case WHEN royalty_amount > 0 THEN royalty_receive_address END AS
          royalty_fee_receive_address
          ,case WHEN royalty_amount > 0 AND a.original_currency_contract =
          '0x0000000000000000000000000000000000000000' THEN 'ETH'
          WHEN royalty_amount > 0 THEN t1.symbol
          END AS royalty_fee_currency_symbol
          ,a.tx_hash || '-' || a.attempt_amount::STRING || '-' || a.nft_token_id || '-' ||  concat('0x',substr(seller,3,40)) || '-' ||
          order_type_id::STRING || '-' || cast(row_number () over (partition BY a.tx_hash order BY sub_idx) AS
          STRING) AS unique_trade_id,
          a.zone
      FROM p3_txn_level a
        inner join {{ source('ethereum', 'transactions') }} tx
            ON tx.hash = a.tx_hash
            {% if NOT is_incremental() %}
            AND tx.block_number > 14801608
            {% endif %}
            {% if is_incremental() %}
            AND tx.block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN {{ ref('nft_aggregators') }} agg
            ON agg.contract_address = tx.to AND agg.blockchain = 'ethereum'
        LEFT JOIN {{ ref('nft_ethereum_aggregators_markers') }} agg_m
                ON LEFT(tx.data, CHARINDEX(agg_m.hash_marker, tx.data) + LENGTH(agg_m.hash_marker)) LIKE '%' || agg_m.hash_marker
        LEFT JOIN {{ ref('tokens_nft') }} n
            ON n.contract_address = nft_contract_address AND n.blockchain = 'ethereum'
        LEFT JOIN {{ source('erc721_ethereum', 'evt_transfer') }} erct2 ON erct2.evt_block_time=a.block_time
            AND nft_contract_address=erct2.contract_address
            AND erct2.evt_tx_hash=a.tx_hash
            AND erct2.tokenId=a.nft_token_id
            AND erct2.from=concat('0x',substr(buyer,3,40))
            {% if NOT is_incremental() %}
            AND erct2.evt_block_number > 14801608
            {% endif %}
            {% if is_incremental() %}
            AND erct2.evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN {{ source('erc1155_ethereum', 'evt_transfersingle') }} erct3 ON erct3.evt_block_time=a.block_time
            AND nft_contract_address=erct3.contract_address
            AND erct3.evt_tx_hash=a.tx_hash
            AND erct3.id=a.nft_token_id
            AND erct3.from=concat('0x',substr(buyer,3,40))
            {% if NOT is_incremental() %}
            AND erct3.evt_block_number > 14801608
            {% endif %}
            {% if is_incremental() %}
            AND erct3.evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN {{ ref('tokens_erc20') }} t1
            ON t1.contract_address =
                case WHEN a.original_currency_contract = '0x0000000000000000000000000000000000000000'
                THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE a.original_currency_contract
                END
            AND t1.blockchain = 'ethereum'
          LEFT JOIN {{ source('prices', 'usd') }} p1
            ON p1.contract_address =
                case WHEN a.original_currency_contract = '0x0000000000000000000000000000000000000000'
                THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE a.original_currency_contract
                END
            AND p1.minute = date_trunc('minute', a.block_time)
            AND p1.blockchain = 'ethereum'
            {% if is_incremental() %}
            AND p1.minute >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            )

,p4_call AS (SELECT 'match_orders' AS main_type
          ,'match_orders' AS sub_type
          ,idx AS sub_idx
          ,get_json_object(get_json_object(c.orders[0], "$.parameters"), "$.zone") AS zone
          ,get_json_object(each, "$.offerer") AS offerer
          ,get_json_object(get_json_object(each, "$.item"),"$.token") AS offer_token
          ,get_json_object(get_json_object(each, "$.item"),"$.amount") AS offer_amount
          ,get_json_object(get_json_object(each, "$.item"),"$.itemType") AS offer_item_type
          ,get_json_object(get_json_object(each, "$.item"),"$.identifier") AS offer_identifier
          ,get_json_object(get_json_object(each, "$.item"),"$.recipient") AS recipient
          ,c.call_tx_hash AS tx_hash
          ,c.call_block_time AS block_time
          ,c.call_block_number AS block_number
          ,c.contract_address AS exchange_contract_address
     FROM (SELECT *, posexplode(output_executions) AS (idx, each) from {{ source('seaport_ethereum', 'Seaport_call_matchOrders') }}
     {% if is_incremental() %} -- this filter will only be applied ON an incremental run
     where call_block_time >= (SELECT max(block_time) FROM {{ this }})
     {% endif %}
     ) c
    where call_success

    union all
    SELECT 'match_advanced_orders' AS main_type
          ,'match_advanced_orders' AS sub_type
          ,idx AS sub_idx
          ,get_json_object(get_json_object(c.advancedOrders[0], "$.parameters"), "$.zone") AS zone
          ,get_json_object(each, "$.offerer") AS offerer
          ,get_json_object(get_json_object(each, "$.item"),"$.token") AS offer_token
          ,get_json_object(get_json_object(each, "$.item"),"$.amount") AS offer_amount
          ,get_json_object(get_json_object(each, "$.item"),"$.itemType") AS offer_item_type
          ,get_json_object(get_json_object(each, "$.item"),"$.identifier") AS offer_identifier
          ,get_json_object(get_json_object(each, "$.item"),"$.recipient") AS recipient
          ,c.call_tx_hash AS tx_hash
          ,c.call_block_time AS block_time
          ,c.call_block_number AS block_number
          ,c.contract_address AS exchange_contract_address
    FROM (SELECT *, posexplode(output_executions) AS (idx, each) from {{ source('seaport_ethereum', 'Seaport_call_matchAdvancedOrders') }}
    {% if is_incremental() %} -- this filter will only be applied ON an incremental run
    where call_block_time >= (SELECT max(block_time) FROM {{ this }})
    {% endif %}
    ) c
    where call_success)


  ,p4_add_rn AS (
    SELECT max(case WHEN fee_royalty_yn = 'price' THEN offerer END) over (partition BY tx_hash) AS price_offerer
          ,max(case WHEN fee_royalty_yn = 'price' THEN recipient END) over (partition BY tx_hash) AS price_recipient
          ,max(case WHEN fee_royalty_yn = 'price' THEN offer_token END) over (partition BY tx_hash) AS price_token
          ,max(case WHEN fee_royalty_yn = 'price' THEN offer_amount END) over (partition BY tx_hash) / nft_transfer_count AS price_amount
          ,max(case WHEN fee_royalty_yn = 'price' THEN offer_item_type END) over (partition BY tx_hash) AS price_item_type
          ,max(case WHEN fee_royalty_yn = 'price' THEN offer_identifier END) over (partition BY tx_hash) AS price_id
          ,max(case WHEN fee_royalty_yn = 'fee' THEN offerer END) over (partition BY tx_hash) AS fee_offerer
          ,max(case WHEN fee_royalty_yn = 'fee' THEN recipient END) over (partition BY tx_hash) AS fee_recipient
          ,max(case WHEN fee_royalty_yn = 'fee' THEN offer_token END) over (partition BY tx_hash) AS fee_token
          ,max(case WHEN fee_royalty_yn = 'fee' THEN offer_amount END) over (partition BY tx_hash) / nft_transfer_count AS fee_amount
          ,max(case WHEN fee_royalty_yn = 'fee' THEN offer_item_type END) over (partition BY tx_hash) AS fee_item_type
          ,max(case WHEN fee_royalty_yn = 'fee' THEN offer_identifier END) over (partition BY tx_hash) AS fee_id
          ,max(case WHEN fee_royalty_yn = 'royalty' THEN offerer END) over (partition BY tx_hash) AS royalty_offerer
          ,max(case WHEN fee_royalty_yn = 'royalty' THEN recipient END) over (partition BY tx_hash) AS royalty_recipient
          ,max(case WHEN fee_royalty_yn = 'royalty' THEN offer_token END) over (partition BY tx_hash) AS royalty_token
          ,max(case WHEN fee_royalty_yn = 'royalty' THEN offer_amount END) over (partition BY tx_hash) / nft_transfer_count AS royalty_amount
          ,max(case WHEN fee_royalty_yn = 'royalty' THEN offer_item_type END) over (partition BY tx_hash) AS royalty_item_type
          ,max(case WHEN fee_royalty_yn = 'royalty' THEN offer_identifier END) over (partition BY tx_hash) AS royalty_id
          ,a.*
      FROM (SELECT case WHEN fee_royalty_idx = 1 THEN 'price'
                        WHEN fee_royalty_idx = 2 THEN 'fee'
                        WHEN fee_royalty_idx = 3 THEN 'royalty'
                   END AS fee_royalty_yn
                  ,a.*
              FROM (SELECT count(case WHEN offer_item_type in ('2', '3') THEN 1 END) over (partition BY tx_hash) AS nft_transfer_count
                          ,sum(case WHEN offer_item_type in ('0', '1') THEN 1 END) over (partition BY tx_hash order BY sub_idx) AS fee_royalty_idx
                          ,a.*
                      FROM p4_call a
                    ) a
             where nft_transfer_count > 0  -- some of trades without NFT happens in match_order
            ) a
)

,p4_transfer_level AS (
    SELECT a.main_type
          ,a.sub_idx
          ,a.tx_hash
          ,a.block_time
          ,a.block_number
          ,a.zone
          ,a.exchange_contract_address
          ,offer_token AS nft_address
          ,offer_identifier AS nft_token_id
          ,recipient AS buyer
          ,offerer AS seller
          ,offer_item_type AS offer_item_type
          ,offer_identifier AS nft_token_id_dcnt
          ,offer_amount AS nft_token_amount
          ,price_token AS price_token
          ,price_item_type AS price_item_type
          ,price_amount AS price_amount
          ,fee_amount AS fee_amount
          ,royalty_amount AS royalty_amount
          ,price_amount AS evt_price_amount
          ,fee_amount AS evt_fee_amount
          ,royalty_amount AS evt_royalty_amount
          ,fee_token AS evt_fee_token
          ,royalty_token AS evt_royalty_token
          ,fee_recipient AS evt_fee_recipient
          ,royalty_recipient AS evt_royalty_recipient
          ,coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) AS attempt_amount
          ,0 AS revert_amount
          ,false AS reverted
          ,'' AS offer_order_type
          ,'Private Sales' AS order_type
          ,'Buy' AS purchase_method
          ,nft_transfer_count
      FROM p4_add_rn a
     where offer_item_type in ('2', '3')
)

,p4_seaport_transfers AS (
          SELECT
          'ethereum' AS blockchain
          ,'seaport' AS project
          ,'v1' AS version
          ,TRY_CAST(date_trunc('DAY', a.block_time) AS date) AS block_date
          ,a.block_time
          ,a.block_number
          ,a.nft_token_id AS token_id
          ,n.name AS collection
          ,a.attempt_amount / power(10,t1.decimals) * p1.price AS amount_usd
          ,case WHEN offer_item_type = '2' THEN 'erc721'
                WHEN offer_item_type = '3' THEN 'erc1155'
           END AS token_standard
          ,case WHEN order_type = 'Bulk Purchase' THEN 'Bulk Purchase'
                WHEN nft_transfer_count = 1 THEN 'Single Item Trade'
                ELSE 'Bundle Trade'
           END AS trade_type
          ,nft_token_amount AS number_of_items
          ,a.purchase_method AS trade_category
          ,'Trade' AS evt_type
          ,concat('0x',substr(seller,3,40)) AS seller
          , CASE WHEN concat('0x',substr(buyer,3,40))=agg.contract_address THEN COALESCE(erct2.to, erct3.to)
            ELSE concat('0x',substr(buyer,3,40)) END AS buyer
          ,a.attempt_amount / power(10,t1.decimals) AS amount_original
          ,a.attempt_amount AS amount_raw
          ,case WHEN concat('0x',substr(a.price_token,3,40)) =
          '0x0000000000000000000000000000000000000000' THEN 'ETH'
                ELSE t1.symbol
           END AS currency_symbol
          ,case WHEN concat('0x',substr(a.price_token,3,40)) =
          '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE concat('0x',substr(a.price_token,3,40))
           END AS currency_contract
          ,concat('0x',substr(a.nft_address,3,40)) AS nft_contract_address
          ,a.exchange_contract_address AS project_contract_address
          ,coalesce(agg_m.aggregator_name, agg.name) AS aggregator_name
          ,agg.contract_address AS aggregator_address
          ,a.tx_hash
          ,tx.from AS tx_from
          ,tx.to AS tx_to
          ,ROUND((2.5*(a.attempt_amount) / 100),7) AS platform_fee_amount_raw
          ,ROUND((2.5*((a.attempt_amount / power(10,t1.decimals)))/100),7) AS platform_fee_amount
          ,ROUND((2.5*((a.attempt_amount / power(10,t1.decimals)* p1.price))/100),7) AS platform_fee_amount_usd
          ,'2.5' AS platform_fee_percentage
          ,a.evt_royalty_amount AS royalty_fee_amount_raw
          ,a.evt_royalty_amount / power(10,t1.decimals) AS royalty_fee_amount
          ,a.evt_royalty_amount / power(10,t1.decimals) * p1.price AS royalty_fee_amount_usd
          , (a.evt_royalty_amount / a.attempt_amount * 100)::STRING  AS royalty_fee_percentage
          ,case WHEN evt_royalty_amount > 0 THEN concat('0x',substr(evt_royalty_recipient,3,40)) END AS
          royalty_fee_receive_address
          ,case WHEN evt_royalty_amount > 0 AND concat('0x',substr(a.evt_royalty_token,3,40)) =
          '0x0000000000000000000000000000000000000000' THEN 'ETH'
                WHEN evt_royalty_amount > 0 THEN t1.symbol
          END AS royalty_fee_currency_symbol
          ,a.tx_hash || '-' || a.nft_token_id || '-' || a.attempt_amount::STRING || '-' || concat('0x',substr(seller,3,40)) || '-' || cast(row_number () over (partition BY a.tx_hash order BY sub_idx) AS
          STRING) AS unique_trade_id,
          a.zone
    FROM p4_transfer_level a
    inner join {{ source('ethereum', 'transactions') }} tx
        ON tx.hash = a.tx_hash
        {% if NOT is_incremental() %}
        AND tx.block_number > 14801608
        {% endif %}
        {% if is_incremental() %}
        AND tx.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ source('erc721_ethereum', 'evt_transfer') }} erct2 ON erct2.evt_block_time=a.block_time
        AND concat('0x',substr(a.nft_address,3,40))=erct2.contract_address
        AND erct2.evt_tx_hash=a.tx_hash
        AND erct2.tokenId=a.nft_token_id
        AND erct2.from=concat('0x',substr(buyer,3,40))
        {% if NOT is_incremental() %}
        AND erct2.evt_block_number > 14801608
        {% endif %}
        {% if is_incremental() %}
        AND erct2.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ source('erc1155_ethereum', 'evt_transfersingle') }} erct3 ON erct3.evt_block_time=a.block_time
        AND concat('0x',substr(a.nft_address,3,40))=erct3.contract_address
        AND erct3.evt_tx_hash=a.tx_hash
        AND erct3.id=a.nft_token_id
        AND erct3.from=concat('0x',substr(buyer,3,40))
        {% if NOT is_incremental() %}
        AND erct3.evt_block_number > 14801608
        {% endif %}
        {% if is_incremental() %}
        AND erct3.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        LEFT JOIN {{ ref('nft_ethereum_aggregators_markers') }} agg_m
                ON LEFT(tx.data, CHARINDEX(agg_m.hash_marker, tx.data) + LENGTH(agg_m.hash_marker)) LIKE '%' || agg_m.hash_marker
    LEFT JOIN {{ ref('nft_aggregators') }} agg
        ON agg.contract_address = tx.to AND agg.blockchain = 'ethereum'
    LEFT JOIN {{ ref('tokens_nft') }} n
        ON n.contract_address = concat('0x',substr(a.nft_address,3,40)) AND n.blockchain = 'ethereum'
    LEFT JOIN {{ ref('tokens_erc20') }} t1
        ON t1.contract_address =
            case WHEN concat('0x',substr(a.price_token,3,40)) = '0x0000000000000000000000000000000000000000'
            THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE concat('0x',substr(a.price_token,3,40))
            END
        AND t1.blockchain = 'ethereum'
        LEFT JOIN {{ source('prices', 'usd') }} p1
        ON p1.contract_address =
            case WHEN concat('0x',substr(a.price_token,3,40)) = '0x0000000000000000000000000000000000000000'
            THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE concat('0x',substr(a.price_token,3,40))
            END
        AND p1.minute = date_trunc('minute', a.block_time)
        AND p1.blockchain = 'ethereum'
        {% if is_incremental() %}
        AND p1.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
            )

SELECT * FROM p1_seaport_transfers
    union all
SELECT *
      FROM p2_seaport_transfers
    union all
SELECT *
      FROM p3_seaport_transfers
    union all
SELECT *
      FROM p4_seaport_transfers
