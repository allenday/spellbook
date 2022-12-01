{{ config(
    alias = 'base_pairs',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index', 'sub_type', 'sub_idx'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                            "project",
                            "seaport",
                            \'["sohawk"]\') }}'
    )
}}

{% set c_seaport_first_date = "2022-06-01" %}

with iv_offer_consideration AS (
    SELECT evt_block_time AS block_time
            ,evt_block_number AS block_number
            ,evt_tx_hash AS tx_hash
            ,evt_index
            ,'offer' AS sub_type
            ,offer_idx + 1 AS sub_idx
            ,case offer[0]:itemType
                WHEN '0' THEN 'native'
                WHEN '1' THEN 'erc20'
                WHEN '2' THEN 'erc721'
                WHEN '3' THEN 'erc1155'
                ELSE 'etc'
            END AS offer_first_item_type
            ,case consideration[0]:itemType
                WHEN '0' THEN 'native'
                WHEN '1' THEN 'erc20'
                WHEN '2' THEN 'erc721'
                WHEN '3' THEN 'erc1155'
                ELSE 'etc'
            END AS consideration_first_item_type
            ,offerer AS sender
            ,recipient AS receiver
            ,zone
            ,offer_item:token AS token_contract_address
            ,offer_item:amount::numeric(38) AS original_amount
            ,case offer_item:itemType
                WHEN '0' THEN 'native'
                WHEN '1' THEN 'erc20'
                WHEN '2' THEN 'erc721'
                WHEN '3' THEN 'erc1155'
                ELSE 'etc'
            END AS item_type
            ,offer_item:identifier AS token_id
            ,contract_address AS platform_contract_address
            ,size(offer) AS offer_cnt
            ,size(consideration) AS consideration_cnt
            ,case WHEN recipient = '0x0000000000000000000000000000000000000000' THEN true
                ELSE false
            END AS is_private
    FROM
    (
        SELECT consideration
            , contract_address
            , evt_block_number
            , evt_block_time
            , evt_index
            , evt_tx_hash
            , offer
            , offerer
            -- , orderHash
            , recipient
            , zone
            , posexplode(offer) AS (offer_idx, offer_item)
        FROM {{ source('seaport_polygon', 'Seaport_evt_OrderFulfilled') }}
        {% if NOT is_incremental() %}
        where evt_block_time >= date '{{c_seaport_first_date}}'  -- seaport first txn
        {% endif %}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    )
    union all
    SELECT evt_block_time AS block_time
            ,evt_block_number AS block_number
            ,evt_tx_hash AS tx_hash
            ,evt_index
            ,'consideration' AS sub_type
            ,consideration_idx + 1 AS sub_idx
            ,case offer[0]:itemType
                WHEN '0' THEN 'native'
                WHEN '1' THEN 'erc20'
                WHEN '2' THEN 'erc721'
                WHEN '3' THEN 'erc1155'
                ELSE 'etc'
            END AS offer_first_item_type
            ,case consideration[0]:itemType
                WHEN '0' THEN 'native'
                WHEN '1' THEN 'erc20'
                WHEN '2' THEN 'erc721'
                WHEN '3' THEN 'erc1155'
                ELSE 'etc'
            END AS consideration_first_item_type
            ,recipient AS sender
            ,consideration_item:recipient AS receiver
            ,zone
            ,consideration_item:token AS token_contract_address
            ,consideration_item:amount::numeric(38) AS original_amount
            ,case consideration_item:itemType
                WHEN '0' THEN 'native'
                WHEN '1' THEN 'erc20'
                WHEN '2' THEN 'erc721'
                WHEN '3' THEN 'erc1155'
                ELSE 'etc' -- actually NOT exists
            END AS item_type
            ,consideration_item:identifier AS token_id
            ,contract_address AS platform_contract_address
            ,size(offer) AS offer_cnt
            ,size(consideration) AS consideration_cnt
            ,case WHEN recipient = '0x0000000000000000000000000000000000000000' THEN true
                ELSE false
            END AS is_private
    FROM
    (
        SELECT consideration
            , contract_address
            , evt_block_number
            , evt_block_time
            , evt_index
            , evt_tx_hash
            , offer
            -- , offerer
            -- , orderHash
            , recipient
            , zone
            ,posexplode(consideration) AS (consideration_idx, consideration_item)
        FROM {{ source('seaport_polygon', 'Seaport_evt_OrderFulfilled') }}
        {% if NOT is_incremental() %}
        where evt_block_time >= date '{{c_seaport_first_date}}'  -- seaport first txn
        {% endif %}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    )
)
,iv_base_pairs AS (
    SELECT a.*
            ,try_cast(date_trunc('day', a.block_time) AS date) AS block_date
            ,case WHEN offer_first_item_type = 'erc20' THEN 'offer accepted'
                WHEN offer_first_item_type in ('erc721', 'erc1155') THEN 'buy'
                ELSE 'etc' -- some txns has no nfts
            END AS order_type
            ,case WHEN offer_first_item_type = 'erc20' AND sub_type = 'offer' AND item_type = 'erc20' THEN true
                WHEN offer_first_item_type in ('erc721', 'erc1155') AND sub_type = 'consideration' AND item_type in ('native', 'erc20') THEN true
                ELSE false
            END is_price
            ,case WHEN offer_first_item_type = 'erc20' AND sub_type = 'consideration' AND eth_erc_idx = 0 THEN true  -- offer accepted has no price at all. it has to be calculated.
                WHEN offer_first_item_type in ('erc721', 'erc1155') AND sub_type = 'consideration' AND eth_erc_idx = 1 THEN true
                ELSE false
            END is_netprice
            ,case WHEN offer_first_item_type = 'erc20' AND sub_type = 'consideration' AND eth_erc_idx = 1 THEN true
                WHEN offer_first_item_type in ('erc721', 'erc1155') AND sub_type = 'consideration' AND eth_erc_idx = 2 THEN true
                ELSE false
            END is_platform_fee
            ,case WHEN offer_first_item_type = 'erc20' AND sub_type = 'consideration' AND eth_erc_idx > 1 THEN true
                WHEN offer_first_item_type in ('erc721', 'erc1155') AND sub_type = 'consideration' AND eth_erc_idx > 2 THEN true
                ELSE false
            END is_creator_fee
            ,case WHEN offer_first_item_type = 'erc20' AND sub_type = 'consideration' AND eth_erc_idx > 1 THEN eth_erc_idx - 1
                WHEN offer_first_item_type in ('erc721', 'erc1155') AND sub_type = 'consideration' AND eth_erc_idx > 2 THEN eth_erc_idx - 2
            END creator_fee_idx
            ,case WHEN offer_first_item_type = 'erc20' AND sub_type = 'consideration' AND item_type in ('erc721', 'erc1155') THEN true
                WHEN offer_first_item_type in ('erc721', 'erc1155') AND sub_type = 'offer' AND item_type in ('erc721', 'erc1155') THEN true
                ELSE false
            END is_traded_nft
            ,case WHEN offer_first_item_type in ('erc721', 'erc1155') AND sub_type = 'consideration' AND item_type in ('erc721', 'erc1155') THEN true
                ELSE false
            END is_moved_nft
    FROM
    (
        SELECT a.*
            ,case WHEN item_type in ('native', 'erc20') THEN sum(case WHEN item_type in ('native', 'erc20') THEN 1 END) OVER (PARTITION BY tx_hash, evt_index, sub_type ORDER BY sub_idx) END AS eth_erc_idx
            ,sum(case WHEN offer_first_item_type = 'erc20' AND sub_type = 'consideration' AND item_type in ('erc721', 'erc1155') THEN 1
                        WHEN offer_first_item_type in ('erc721', 'erc1155') AND sub_type = 'offer' AND item_type in ('erc721', 'erc1155') THEN 1
                END) OVER (PARTITION BY tx_hash, evt_index) AS nft_cnt
            ,sum(case WHEN offer_first_item_type = 'erc20' AND sub_type = 'consideration' AND item_type in ('erc721') THEN 1
                        WHEN offer_first_item_type in ('erc721', 'erc1155') AND sub_type = 'offer' AND item_type in ('erc721') THEN 1
                END) OVER (PARTITION BY tx_hash, evt_index) AS erc721_cnt
            ,sum(case WHEN offer_first_item_type = 'erc20' AND sub_type = 'consideration' AND item_type in ('erc1155') THEN 1
                        WHEN offer_first_item_type in ('erc721', 'erc1155') AND sub_type = 'offer' AND item_type in ('erc1155') THEN 1
                END) OVER (PARTITION BY tx_hash, evt_index) AS erc1155_cnt
        FROM iv_offer_consideration a
    ) a
)
SELECT *
FROM iv_base_pairs
;