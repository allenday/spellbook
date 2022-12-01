{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "superrare",
                                \'["cat"]\') }}'
    )
}}

-- raw data table with all sales ON superrare platform -- both primary AND secondary
with all_superrare_sales AS (
    SELECT  evt_block_time
            , `_originContract` AS contract_address
            , `_tokenId` AS tokenId
            , `_seller` AS seller
            , `_buyer` AS buyer
            , `_amount` AS amount
            , evt_tx_hash
            , '' AS currencyAddress
    FROM {{ source('superrare_ethereum', 'SuperRareMarketAuction_evt_Sold') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all

    SELECT evt_block_time
            , contract_address
            , `_tokenId`
            , `_seller`
            , `_buyer`
            , `_amount`
            , evt_tx_hash
            , ''
    FROM {{ source('superrare_ethereum', 'SuperRare_evt_Sold') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all

    SELECT evt_block_time
            , `_originContract` AS contract_address
            , `_tokenId`
            , `_seller`
            , `_bidder`
            , `_amount`
            , evt_tx_hash
            , ''
    FROM {{ source('superrare_ethereum', 'SuperRareMarketAuction_evt_AcceptBid') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all

    SELECT evt_block_time
            , contract_address
            , `_tokenId`
            , `_seller`
            , `_bidder`
            , `_amount`
            , evt_tx_hash
            , ''
    FROM {{ source('superrare_ethereum', 'SuperRare_evt_AcceptBid') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all

    SELECT evt_block_time
            , `_originContract`
            , `_tokenId`
            , `_seller`
            , `_bidder`
            , `_amount`
            , evt_tx_hash
            , `_currencyAddress`
    FROM {{ source('superrare_ethereum', 'SuperRareBazaar_evt_AcceptOffer') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all

    SELECT evt_block_time
            , `_contractAddress`
            , `_tokenId`
            , `_seller`
            , `_bidder`
            , `_amount`
            , evt_tx_hash
            , `_currencyAddress`
    FROM {{ source('superrare_ethereum', 'SuperRareBazaar_evt_AuctionSettled') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all

    SELECT evt_block_time
            , `_originContract`
            , `_tokenId`
            , `_seller`
            , `_buyer`
            , `_amount`
            , evt_tx_hash
            , `_currencyAddress`
    FROM {{ source('superrare_ethereum', 'SuperRareBazaar_evt_Sold') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all

    SELECT  block_time
            , concat('0x',SUBSTRING(topic2 FROM 27 for 40)) AS contract_address
            , bytea2numeric_v2(SUBSTRING(topic4 FROM 3)) AS token_id
            , lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656') AS seller -- all sent FROM auction house contract
            , concat('0x',SUBSTRING(topic3 FROM 27 for 40)) AS buyer
            , bytea2numeric_v2(SUBSTRING(data FROM 67 for 64)) AS amount
            , tx_hash
            , ''
    FROM {{ source('ethereum', 'logs') }}
    where contract_address = lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656')
        AND topic1 = lower('0xea6d16c6bfcad11577aef5cc6728231c9f069ac78393828f8ca96847405902a9')
        {% if is_incremental() %}
        AND block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

    union all

    SELECT block_time
            , concat('0x',SUBSTRING(topic2 FROM 27 for 40)) AS contract_address
            , bytea2numeric_v2(SUBSTRING(data FROM 67 for 64)) AS token_id
            , concat('0x',SUBSTRING(topic4 FROM 27 for 40)) AS seller
            , concat('0x',SUBSTRING(topic3 FROM 27 for 40)) AS buyer
            , bytea2numeric_v2(SUBSTRING(data FROM 3 for 64)) AS amount
            , tx_hash
            , ''
    FROM {{ source('ethereum', 'logs') }}
    where contract_address =  lower('0x65b49f7aee40347f5a90b714be4ef086f3fe5e2c')
        AND topic1 in (lower('0x2a9d06eec42acd217a17785dbec90b8b4f01a93ecd8c127edd36bfccf239f8b6')
                        , lower('0x5764dbcef91eb6f946584f4ea671217c686fa7e858ce4f9f42d08422b86556a9')
                      )
        {% if is_incremental() %}
        AND block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)
-- some items are sold in RARE currency ON superrare. NOT available ON coinpaprika, so using dex data to be able to convert to USD. usuing weekly average since dex data isn't fully populated ON v2 right now (~1 / 8 of data vs. v1). switch back to daily once full data is available
, rare_token_price_eth AS (
    SELECT
        date_trunc('week', block_time) AS week,
        avg(
            case
            WHEN token_bought_symbol like '%ETH%' THEN token_bought_amount * 1.0 / nullif(token_sold_amount, 0)
            ELSE token_sold_amount * 1.0 / nullif(token_bought_amount, 0)
            END
        ) AS average_price_that_day_eth_per_rare
    FROM
        {{ ref('dex_trades') }}
    where
        -- RARE trades
        blockchain = 'ethereum'
        AND (
            token_bought_address = lower('0xba5bde662c17e2adff1075610382b9b691296350')
            or token_sold_address = lower('0xba5bde662c17e2adff1075610382b9b691296350')
        )
        AND (
            token_bought_symbol like '%ETH%'
            or token_sold_symbol like '%ETH%'
        )
        AND case
            WHEN token_bought_symbol like '%ETH%' THEN token_bought_amount * 1.0 / nullif(token_sold_amount, 0)
            ELSE token_sold_amount * 1.0 / nullif(token_bought_amount, 0)
            END < 0.001
        {% if is_incremental() %}
        AND block_date >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    GROUP BY
        1
)
, transfers_for_tokens_sold_from_auction AS (
        SELECT evt.contract_address
            , evt.tokenId
            , evt.evt_tx_hash
            , evt.to
            , evt.from
            , lag(evt.from) OVER (PARTITION BY evt.contract_address, evt.tokenId ORDER BY evt.evt_block_time ASC) AS previous_owner
            , case  WHEN evt.from = lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656')
                        THEN 'From Auction House'
                    WHEN evt.to = lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656')
                        THEN 'To Auction House'
                ELSE '' END AS auction_house_flag
            , ROW_NUMBER() OVER (PARTITION BY evt.contract_address, evt.tokenId ORDER BY evt.evt_block_time DESC) AS transaction_rank
    FROM {{ source('erc721_ethereum', 'evt_transfer') }} evt
    inner join all_superrare_sales tsfa
        ON evt.contract_address = tsfa.contract_address
        AND evt.tokenId = tsfa.tokenId
        AND tsfa.seller = lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656')
    {% if is_incremental() %}
    where evt.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
SELECT
    'ethereum' AS blockchain,
    'superrare' AS project,
    'v1' AS version,
    cast(date_trunc('day', a.evt_block_time) AS date) AS block_date,
    a.evt_block_time AS block_time,
    a.tokenId AS token_id,
    '' AS collection,
    case
    WHEN a.currencyAddress = '0xba5bde662c17e2adff1075610382b9b691296350' THEN (a.amount / 1e18) * average_price_that_day_eth_per_rare * ep.price
    ELSE (a.amount / 1e18) * ep.price
    END AS amount_usd,
    case
    WHEN a.contract_address = '0x41a322b28d0ff354040e2cbc676f0320d8c8850d' THEN 'erc20'
    ELSE 'erc721'
    END AS token_standard,
    'Single Item Trade' AS trade_type,
    1 AS number_of_items,
    'Buy' AS trade_category,
    'Trade' AS evt_type,
    a.seller AS seller,
    a.buyer AS buyer,
    (a.amount / 1e18) AS amount_original,
    a.amount AS amount_raw,
    case
    WHEN a.currencyAddress = '0xba5bde662c17e2adff1075610382b9b691296350' THEN 'RARE'
    ELSE 'ETH' -- only RARE AND ETH possible
    END AS currency_symbol,
    case
    WHEN a.currencyAddress = '0xba5bde662c17e2adff1075610382b9b691296350' THEN '0xba5bde662c17e2adff1075610382b9b691296350'
    ELSE '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    END AS currency_contract,
    a.contract_address AS nft_contract_address,
    '' AS project_contract_address,
    '' AS aggregator_name,
    '' AS aggregator_address,
    a.evt_tx_hash AS tx_hash,
    t.block_number AS block_number,
    t.from AS tx_from,
    t.to AS tx_to,
    ROUND((3 * (a.amount) / 100), 7) AS platform_fee_amount_raw,
    ROUND((3 * ((a.amount / 1e18)) / 100), 7) platform_fee_amount,
    case
    WHEN a.currencyAddress = '0xba5bde662c17e2adff1075610382b9b691296350' THEN ROUND(
        (
        3 * (
            (a.amount / 1e18) * average_price_that_day_eth_per_rare * ep.price
        ) / 100
        ),
        7
    )
    ELSE ROUND((3 * ((a.amount / 1e18) * ep.price) / 100), 7)
    END AS platform_fee_amount_usd,
    CAST('3' AS DOUBLE) AS platform_fee_percentage,
    case
    WHEN evt.to = po.previous_owner THEN 'Primary' -- auctions
    WHEN evt.to = seller THEN 'Primary'
    WHEN erc20.to = seller THEN 'Primary'
    ELSE 'Secondary'
    END AS superrare_sale_type,
    case
    WHEN evt.to != po.previous_owner
    AND evt.to != seller
    AND erc20.to != seller -- secondary sale
    THEN ROUND((10 * (a.amount) / 100), 7)
    ELSE NULL
    END AS royalty_fee_amount_raw,
    case
    WHEN evt.to != po.previous_owner
    AND evt.to != seller
    AND erc20.to != seller -- secondary sale
    THEN ROUND((10 * ((a.amount / 1e18)) / 100), 7)
    ELSE NULL
    END AS royalty_fee_amount,
    case
    WHEN a.currencyAddress = '0xba5bde662c17e2adff1075610382b9b691296350'
    AND evt.to != po.previous_owner
    AND evt.to != seller
    AND erc20.to != seller -- secondary sale AND rare
    THEN ROUND(
        (
        10 * (
            (a.amount / 1e18) * average_price_that_day_eth_per_rare * ep.price
        ) / 100
        ),
        7
    )
    WHEN evt.to != po.previous_owner
    AND evt.to != seller
    AND erc20.to != seller -- secondary sales
    THEN ROUND((10 * ((a.amount / 1e18) * ep.price) / 100), 7)
    ELSE NULL
    END AS royalty_fee_amount_usd,
    CAST('10' AS DOUBLE) AS royalty_fee_percentage,
    case
    WHEN evt.to is NOT NULL THEN evt.to
    ELSE erc20.to
    END AS royalty_fee_receive_address,
    case
    WHEN a.currencyAddress = '0xba5bde662c17e2adff1075610382b9b691296350' THEN 'RARE'
    ELSE 'ETH' -- only RARE AND ETH possible
    END AS royalty_fee_currency_symbol,
    'superrare' || '-' || a.evt_tx_hash || '-' || CAST(a.tokenId AS VARCHAR(100)) || '-' || CAST(a.seller AS VARCHAR(100)) || '-' || COALESCE(a.contract_address) || '-' || 'Trade' AS unique_trade_id
FROM all_superrare_sales a
left outer join
    (
        SELECT
            minute
            , price
        FROM {{ source('prices', 'usd') }}
        where blockchain = 'ethereum'
            AND symbol = 'WETH'
            {% if is_incremental() %}
            AND minute >= date_trunc("day", now() - interval '1 week')
            {% endif %}
    ) ep
    ON date_trunc('minute', a.evt_block_time) = ep.minute
left outer join rare_token_price_eth rp
    ON date_trunc('week', a.evt_block_time) = rp.week
inner join {{ source('ethereum', 'transactions') }} t
    ON a.evt_tx_hash = t.hash
    {% if is_incremental() %}
    AND t.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left outer join {{ source('erc721_ethereum', 'evt_transfer') }} evt ON evt.contract_address = a.contract_address
    AND evt.tokenId = a.tokenId
    AND evt.from = '0x0000000000000000000000000000000000000000'
    {% if is_incremental() %}
    AND evt.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left outer join {{ source('erc20_ethereum', 'evt_transfer') }} erc20 ON erc20.contract_address = a.contract_address
    AND erc20.value = a.tokenId
    AND erc20.from = '0x0000000000000000000000000000000000000000'
    {% if is_incremental() %}
    AND erc20.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left outer join transfers_for_tokens_sold_from_auction po -- if sold FROM auction house previous owner
    ON a.evt_tx_hash = po.evt_tx_hash
where (a.amount / 1e18) > 0
;