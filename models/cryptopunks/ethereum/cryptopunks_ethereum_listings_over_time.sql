{{ config(
        alias ='listings_over_time',
        unique_key='day',
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
    from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkOffered') }}
)
, all_no_longer_for_sale_events (
    SELECT  `punkIndex` AS punk_id
            , 'No Longer For Sale' AS event_type
            , case when evt_tx_hash in (SELECT evt_tx_hash from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBought') }}) then 'Punk Bought'
                    else 'Other'
                end AS event_sub_type
            , null AS listed_price
            , null AS listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkNoLongerForSale') }}
)
, all_buys AS (
    SELECT  `punkIndex` AS punk_id
            , 'Punk Bought' AS event_type
            , 'Punk Bought' AS event_sub_type
            , null AS listed_price
            , null AS listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBought') }}
)
, all_transfers AS (
    SELECT  `punkIndex` AS punk_id
            , 'Punk Transfer' AS event_type
            , 'Punk Transfer' AS event_sub_type
            , null AS listed_price
            , null AS listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkTransfer') }}
)
, base_data AS (
    with all_days  AS (SELECT explode(sequence(to_date('2017-06-23'), to_date(now()), interval 1 day)) as day)
    , all_punk_ids AS (SELECT explode(sequence(0, 9999, 1)) as punk_id)

    SELECT  day
            , punk_id
    from all_days
    full outer join all_punk_ids on true
)
, all_punk_events AS (
    SELECT *
          , row_number() over (partition by punk_id order by evt_block_number asc, evt_index asc ) AS punk_event_index
    from
    (
    SELECT * from all_listings
    union all SELECT * from all_no_longer_for_sale_events
    union all SELECT * from all_buys
    union all SELECT * from all_transfers
    ) a
    order by evt_block_number desc, evt_index desc
)
, aggregated_punk_on_off_data AS (
    SELECT date_trunc('day',a.evt_block_time) AS day
            , a.punk_id
            , case when event_type = 'Listing' then 'Active' else 'Not Listed' end AS listed_bool
    from all_punk_events a
    inner join (    SELECT date_trunc('day', evt_block_time) AS day
                            , punk_id
                            , max(punk_event_index) AS max_event
                    from all_punk_events
                    group by 1,2
                ) b -- max event per punk per day
    on date_trunc('day',a.evt_block_time) = b.day and a.punk_id = b.punk_id and a.punk_event_index = b.max_event
)
SELECT day
        , sum(case when bool_fill_in = 'Active' then 1 else 0 end) AS listed_count
from
(   SELECT c.*
            , last_value(listed_bool,true) over (partition by punk_id order by day asc ) AS bool_fill_in
    from
    (   SELECT a.day
                , a.punk_id
                , listed_bool
        from base_data  a
        left outer join aggregated_punk_on_off_data  b
        on a.day = b.day and a.punk_id = b.punk_id
    ) c
) d
group by 1
order by day desc 