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
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc'
            end AS offer_first_item_type
            ,case consideration[0]:itemType
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc'
            end AS consideration_first_item_type
            ,offerer AS sender
            ,recipient AS receiver
            ,zone
            ,offer_item:token AS token_contract_address
            ,offer_item:amount::numeric(38) AS original_amount
            ,case offer_item:itemType
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc'
            end AS item_type
            ,offer_item:identifier AS token_id
            ,contract_address AS platform_contract_address
            ,size(offer) AS offer_cnt
            ,size(consideration) AS consideration_cnt
            ,case when recipient = '0x0000000000000000000000000000000000000000' then true
                else false
            end AS is_private
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
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc'
            end AS offer_first_item_type
            ,case consideration[0]:itemType
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc'
            end AS consideration_first_item_type
            ,recipient AS sender
            ,consideration_item:recipient AS receiver
            ,zone
            ,consideration_item:token AS token_contract_address
            ,consideration_item:amount::numeric(38) AS original_amount
            ,case consideration_item:itemType
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc' -- actually NOT exists
            end AS item_type
            ,consideration_item:identifier AS token_id
            ,contract_address AS platform_contract_address
            ,size(offer) AS offer_cnt
            ,size(consideration) AS consideration_cnt
            ,case when recipient = '0x0000000000000000000000000000000000000000' then true
                else false
            end AS is_private
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
        FROM {{ source('seaport_polygon','Seaport_evt_OrderFulfilled') }}
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
            ,case when offer_first_item_type = 'erc20' then 'offer accepted'
                when offer_first_item_type in ('erc721','erc1155') then 'buy'
                else 'etc' -- some txns has no nfts
            end AS order_type
            ,case when offer_first_item_type = 'erc20' and sub_type = 'offer' and item_type = 'erc20' then true
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and item_type in ('native','erc20') then true
                else false
            end is_price
            ,case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and eth_erc_idx = 0 then true  -- offer accepted has no price at all. it has to be calculated.
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and eth_erc_idx = 1 then true
                else false
            end is_netprice
            ,case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and eth_erc_idx = 1 then true
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and eth_erc_idx = 2 then true
                else false
            end is_platform_fee
            ,case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and eth_erc_idx > 1 then true
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and eth_erc_idx > 2 then true
                else false
            end is_creator_fee
            ,case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and eth_erc_idx > 1 then eth_erc_idx - 1
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and eth_erc_idx > 2 then eth_erc_idx - 2
            end creator_fee_idx
            ,case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and item_type in ('erc721','erc1155') then true
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'offer' and item_type in ('erc721','erc1155') then true
                else false
            end is_traded_nft
            ,case when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and item_type in ('erc721','erc1155') then true
                else false
            end is_moved_nft
    FROM
    (
        SELECT a.*
            ,case when item_type in ('native','erc20') then sum(case when item_type in ('native','erc20') then 1 end) over (partition by tx_hash, evt_index, sub_type order by sub_idx) end AS eth_erc_idx
            ,sum(case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and item_type in ('erc721','erc1155') then 1
                        when offer_first_item_type in ('erc721','erc1155') and sub_type = 'offer' and item_type in ('erc721','erc1155') then 1
                end) over (partition by tx_hash, evt_index) AS nft_cnt
            ,sum(case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and item_type in ('erc721') then 1
                        when offer_first_item_type in ('erc721','erc1155') and sub_type = 'offer' and item_type in ('erc721') then 1
                end) over (partition by tx_hash, evt_index) AS erc721_cnt
            ,sum(case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and item_type in ('erc1155') then 1
                        when offer_first_item_type in ('erc721','erc1155') and sub_type = 'offer' and item_type in ('erc1155') then 1
                end) over (partition by tx_hash, evt_index) AS erc1155_cnt
        FROM iv_offer_consideration a
    ) a
)
SELECT *
FROM iv_base_pairs
;