{{ config(
        alias ='events',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_trade_id',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cryptopunks",
                                    \'["masquot", "cat"]\') }}'
        )
}}

with cryptopunks_bids_and_sales AS (
    SELECT *
            , ROW_NUMBER() OVER (PARTITION BY punk_id ORDER BY evt_block_number ASC, evt_index ASC) AS punk_id_event_number
    FROM
    (
    SELECT  "PunkBought" AS event_type
            , `punkIndex` AS punk_id
            , `value` / 1e18 AS sale_price
            , NULL AS bid_amount
            , `toAddress` AS to_address
            , `fromAddress` AS from_address
            , NULL AS bid_from_address
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    FROM {{ source('cryptopunks_ethereum', 'CryptoPunksMarket_evt_PunkBought') }}

    union all

    SELECT  "PunkBidEntered" AS event_type
            , `punkIndex` AS punk_id
            , NULL AS sale_price
            , `value` / 1e18 AS bid_amount
            , NULL AS to_address
            , NULL AS from_address
            , `fromAddress` AS bid_from_address
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    FROM {{ source('cryptopunks_ethereum', 'CryptoPunksMarket_evt_PunkBidEntered') }}

    union all

    SELECT  "PunkBidWithdrawn" AS event_type
            , `punkIndex` AS punk_id
            , NULL AS sale_price
            , `value` / 1e18 AS bid_amount
            , NULL AS to_address
            , NULL AS from_address
            , `fromAddress` AS bid_from_address
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    FROM {{ source('cryptopunks_ethereum', 'CryptoPunksMarket_evt_PunkBidWithdrawn') }}
    ) a
)
, bid_sales AS (
    SELECT  "Offer Accepted" AS event_type
            , a.punk_id
            , max(c.bid_amount) AS sale_price -- max bid FROM buyer pre-sale
            , b.`to` AS to_address -- FOR bids accepted, look up who the seller transferred to in the same block with 1 offset index
            , a.from_address
            , a.evt_block_number
            , a.evt_index
            , a.evt_block_time
            , a.evt_tx_hash
    FROM cryptopunks_bids_and_sales a

    join {{ source('cryptopunks_ethereum', 'CryptoPunksMarket_evt_Transfer') }} b
    ON a.from_address = b.`FROM` AND a.evt_block_number = b.evt_block_number AND a.evt_index = (b.evt_index+1)

    left outer join cryptopunks_bids_and_sales c
    ON a.punk_id = c.punk_id AND c.event_type = "PunkBidEntered" AND c.punk_id_event_number < a.punk_id_event_number AND c.bid_from_address = b.`to`

    where a.sale_price = 0 AND a.to_address = '0x0000000000000000000000000000000000000000'
    GROUP BY 1, 2, 4, 5, 6, 7, 8, 9
)
, regular_sales AS (
    SELECT  "Buy" AS event_type
            , a.`punkIndex` AS punk_id
            , a.`value` / 1e18 AS sale_price
            , CASE WHEN a.`toAddress` = '0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2' -- gem
                THEN b.`to`
                ELSE a.`toAddress` END AS to_address
            , a.`fromAddress` AS from_address
            , a.evt_block_number
            , a.evt_index
            , a.evt_block_time
            , a.evt_tx_hash
    FROM {{ source('cryptopunks_ethereum', 'CryptoPunksMarket_evt_PunkBought') }} a
    left outer join {{ source('cryptopunks_ethereum', 'CryptoPunksMarket_evt_PunkTransfer') }} b
    ON a.`punkIndex` = b.`punkIndex`
        AND a.`toAddress` = b.`FROM`
        AND b.`FROM` = '0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2'
        AND a.evt_tx_hash = b.evt_tx_hash

    where a.`value` != 0 or a.`toAddress` != '0x0000000000000000000000000000000000000000' -- only include sales here
)


SELECT  "ethereum" AS blockchain
        , "cryptopunks" AS project
        , "v1" AS `version`
        , a.evt_block_time AS block_time
        , a.punk_id AS token_id
        , "CryptoPunks" AS `collection`
        , a.sale_price * p.price AS amount_usd
        , "erc20" AS token_standard
        , '' AS trade_type
        , 1 AS number_of_items
        , a.event_type AS trade_category
        , from_address AS seller
        , to_address AS buyer
        , "Trade" AS evt_type
        , sale_price AS amount_original
        , sale_price * 1e18 AS amount_raw
        , "ETH" AS currency_symbol
        , "0x0000000000000000000000000000000000000000" AS currency_contract
        , "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb" AS nft_contract_address
        , "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb" AS project_contract_address
        , agg.name AS aggregator_name
        , agg.contract_address AS aggregator_address
        , a.evt_block_number AS block_number
        , a.evt_tx_hash AS tx_hash
        , tx.`FROM` AS tx_from
        , tx.`to` AS tx_to
        , cast(0 AS double) AS platform_fee_amount_raw
        , cast(0 AS double) AS platform_fee_amount
        , cast(0 AS double) AS platform_fee_amount_usd
        , cast(0 AS double) AS platform_fee_percentage
        , cast(0 AS double) AS royalty_fee_amount_raw
        , cast(0 AS double) AS royalty_fee_amount
        , cast(0 AS double) AS royalty_fee_amount_usd
        , cast(0 AS double) AS royalty_fee_percentage
        , '' AS royalty_fee_receive_address
        , '' AS royalty_fee_currency_symbol
        , "cryptopunks" || '-' || a.evt_tx_hash || '-' || a.punk_id || '-' ||  a.from_address || '-' || a.evt_index || '-' || "" AS unique_trade_id
FROM
(   SELECT * FROM bid_sales
    union all
    SELECT * FROM regular_sales
) a

inner join {{ source('ethereum', 'transactions') }} tx ON a.evt_tx_hash = tx.hash
{% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', a.evt_block_time)
    AND p.contract_address = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    AND p.blockchain = "ethereum"
{% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
{% endif %}

LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON agg.contract_address = tx.to

where a.evt_tx_hash NOT in ('0x92488a00dfa0746c300c66a716e6cc11ba9c0f9d40d8c58e792cc7fcebf432d0' -- flash loan https: / /twitter.com/cryptopunksnfts/status/1453903818308083720
                         )
