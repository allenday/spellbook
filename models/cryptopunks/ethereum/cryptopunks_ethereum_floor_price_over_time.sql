{{ config(
        alias ='floor_price_over_time',
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
            , case WHEN `toAddress` = '0x0000000000000000000000000000000000000000' THEN 'Public Listing'
                    ELSE 'Private Listing'
                END AS event_sub_type
            , `minValue` / 1e18 AS listed_price
            , `toAddress` AS listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    FROM {{ source('cryptopunks_ethereum', 'CryptoPunksMarket_evt_PunkOffered') }}
)
, all_no_longer_for_sale_events (
    SELECT  `punkIndex` AS punk_id
            , 'No Longer For Sale' AS event_type
            , case WHEN evt_tx_hash in (SELECT evt_tx_hash FROM {{ source('cryptopunks_ethereum', 'CryptoPunksMarket_evt_PunkBought') }}) THEN 'Punk Bought'
                    ELSE 'Other'
                END AS event_sub_type
            , NULL AS listed_price
            , NULL AS listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    FROM {{ source('cryptopunks_ethereum', 'CryptoPunksMarket_evt_PunkNoLongerForSale') }}
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
    FROM {{ source('cryptopunks_ethereum', 'CryptoPunksMarket_evt_PunkBought') }}
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
    FROM {{ source('cryptopunks_ethereum', 'CryptoPunksMarket_evt_PunkTransfer') }}
)
, base_data AS (
    with all_days  AS (SELECT explode(sequence(to_date('2017-06-23'), to_date(now()), interval 1 day)) AS day)
    , all_punk_ids AS (SELECT explode(sequence(0, 9999, 1)) AS punk_id)

    SELECT  day
            , punk_id
    FROM all_days
    full outer join all_punk_ids ON true
)
, all_punk_events AS (
    SELECT *
          , ROW_NUMBER() OVER (PARTITION BY punk_id ORDER BY evt_block_number ASC, evt_index ASC ) AS punk_event_index
    FROM
    (
    SELECT * FROM all_listings
    union all SELECT * FROM all_no_longer_for_sale_events
    union all SELECT * FROM all_buys
    union all SELECT * FROM all_transfers
    ) a
    ORDER BY evt_block_number DESC, evt_index DESC
)
, aggregated_punk_on_off_data AS (
    SELECT date_trunc('day',a.evt_block_time) AS day
            , a.punk_id
            , listed_price
            , case WHEN event_sub_type = 'Public Listing' THEN 'Active' ELSE 'Not Listed' END AS listed_bool
    FROM all_punk_events a
    inner join (    SELECT date_trunc('day', evt_block_time) AS day
                            , punk_id
                            , max(punk_event_index) AS max_event
                    FROM all_punk_events
                    GROUP BY 1,2
                ) b -- max event per punk per day
    ON date_trunc('day',a.evt_block_time) = b.day AND a.punk_id = b.punk_id AND a.punk_event_index = b.max_event
)

SELECT day
        , floor_price_eth
        , floor_price_eth*1.0*p.price AS floor_price_usd
FROM
(   SELECT day
            , min(price_fill_in) filter (where bool_fill_in = 'Active' AND price_fill_in > 0) AS floor_price_eth
    FROM
    (   SELECT c.*
                , last_value(listed_price,true) OVER (PARTITION BY punk_id ORDER BY day ASC ) AS price_fill_in
                , last_value(listed_bool,true) OVER (PARTITION BY punk_id ORDER BY day ASC ) AS bool_fill_in
        FROM
        (   SELECT a.day
                    , a.punk_id
                    , listed_price
                    , listed_bool
            FROM base_data  a
            left outer join aggregated_punk_on_off_data  b
            ON a.day = b.day AND a.punk_id = b.punk_id
        ) c
    ) d
    GROUP BY 1
) e

LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', e.day)
    AND p.contract_address = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    AND p.blockchain = "ethereum"

ORDER BY day DESC
