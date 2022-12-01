{{ config(
        alias ='current_listings',
        unique_key='punk_id',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cryptopunks",
                                    \'["cat"]\') }}'
        )
}}

with all_listings AS (
    SELECT  `punkIndex` AS punk_id
            , 'Listing' AS event_type
            , case when `toAddress` = '0x0000000000000000000000000000000000000000' then 'Public Listing'
                    else 'Private Listing'
                end AS event_sub_type
            , `minValue` / 1e18 AS listed_price
            , `toAddress` AS listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    FROM {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkOffered') }}
)
, all_no_longer_for_sale_events (
    SELECT  `punkIndex` AS punk_id
            , 'No Longer For Sale' AS event_type
            , case when evt_tx_hash in (SELECT evt_tx_hash FROM cryptopunks_ethereum.CryptoPunksMarket_evt_PunkBought) then 'Punk Bought'
                    else 'Other'
                end AS event_sub_type
            , NULL AS listed_price
            , NULL AS listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    FROM {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkNoLongerForSale') }}
)
, all_buys AS (
    SELECT  `punkIndex` AS punk_id
            , 'Punk Bought' AS event_type
            , 'Punk Bought' AS event_sub_type
            , NULL AS listed_price
            , NULL AS listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    FROM {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBought') }}
)
, all_transfers AS (
    SELECT  `punkIndex` AS punk_id
            , 'Punk Transfer' AS event_type
            , 'Punk Transfer' AS event_sub_type
            , NULL AS listed_price
            , NULL AS listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    FROM {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkTransfer') }}
)

SELECT b.punk_id
        , listed_price
        , evt_block_time AS listing_created_at
FROM
(
    SELECT *
            , row_number() over (partition BY punk_id order BY evt_block_number desc, evt_index desc ) AS punk_event_index
    FROM
    (
    SELECT * FROM all_listings
    union all SELECT * FROM all_no_longer_for_sale_events
    union all SELECT * FROM all_buys
    union all SELECT * FROM all_transfers
    ) a
) b

where punk_event_index = 1 AND event_type = 'Listing' AND event_sub_type = 'Public Listing'
order BY listed_price asc, evt_block_time desc 