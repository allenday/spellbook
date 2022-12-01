{{ config(
        alias = 'events',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'unique_trade_id'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "archipelago",
                                    \'["0xRob", "hildobby"]\') }}'
        )
}}

WITH
    trade_events AS (
        SELECT
            contract_address AS project_contract_address
            , evt_block_number AS block_number
            , evt_block_time AS block_time
            , evt_tx_hash AS tx_hash
            , buyer
            , seller
            , cost AS amount_raw
            , currency AS currency_contract
            , tradeId AS unique_trade_id
        FROM {{ source('archipelago_ethereum', 'ArchipelagoMarket_evt_Trade') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - INTERVAL '1 week')
        {% endif %}
        {% if NOT is_incremental() %}
        WHERE evt_block_time >= '2022-6-20'
        {% endif %}

    ),

    token_events AS (
        SELECT
             evt_block_number AS block_number
            , evt_block_time AS block_time
            , evt_tx_hash AS tx_hash
            , tokenAddress AS nft_contract_address
            , tokenId AS token_id
            , tradeId AS unique_trade_id
        FROM {{ source('archipelago_ethereum', 'ArchipelagoMarket_evt_TokenTrade') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - INTERVAL '1 week')
        {% endif %}
        {% if NOT is_incremental() %}
        WHERE evt_block_time >= '2022-6-20'
        {% endif %}

    ),

    fee_events AS (
        SELECT
             evt_block_number AS block_number
            , evt_block_time AS block_time
            , evt_tx_hash AS tx_hash
            , amount AS fee_amount_raw
            , currency AS fee_currency_contract
            , recipient AS fee_receive_address
            , micros / pow(10,4) AS fee_percentage
            , tradeId AS unique_trade_id
            , CASE WHEN (
                upper(recipient) = upper('0xA76456bb6aBC50FB38e17c042026bc27a95C3314')
                or upper(recipient) = upper('0x1fC12C9f68A6B0633Ba5897A40A8e61ed9274dC9')
                ) THEN true ELSE false END
                AS is_protocol_fee
        FROM {{ source('archipelago_ethereum', 'ArchipelagoMarket_evt_RoyaltyPayment') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - INTERVAL '1 week')
        {% endif %}
        {% if NOT is_incremental() %}
        WHERE evt_block_time >= '2022-6-20'
        {% endif %}

    ),

    tokens_ethereum_nft AS (
        SELECT
            *
        FROM {{ ref('tokens_nft') }}
        WHERE blockchain = 'ethereum'
    ),

    nft_ethereum_aggregators AS (
        SELECT
            *
        FROM {{ ref('nft_aggregators') }}
        WHERE blockchain = 'ethereum'
    ),

    -- enrichments

    trades_with_nft_and_tx AS (
        SELECT
            e.*
            , t.nft_contract_address
            , t.token_id
            , tx.from AS tx_from
            , tx.to  AS tx_to
        FROM trade_events e
        INNER JOIN token_events t
            ON e.block_number = t.block_number AND e.unique_trade_id = t.unique_trade_id
        INNER JOIN {{ source('ethereum', 'transactions') }} tx
            ON e.block_number = tx.block_number AND e.tx_hash = tx.hash
            {% if is_incremental() %}
            AND tx.block_time >= date_trunc("day", now() - INTERVAL '1 week')
            {% endif %}
            {% if NOT is_incremental() %}
            AND tx.block_time >= '2022-6-20'
            {% endif %}
    ),

    platform_fees AS (
        SELECT
            block_number
            ,sum(fee_amount_raw) AS platform_fee_amount_raw
            ,sum(fee_percentage) AS platform_fee_percentage
            ,unique_trade_id
        FROM fee_events
            where is_protocol_fee
        GROUP BY block_number,unique_trade_id
    ),

    royalty_fees AS (
        SELECT
            block_number
            ,sum(fee_amount_raw) AS royalty_fee_amount_raw
            ,sum(fee_percentage) AS royalty_fee_percentage
            , CAST(NULL AS VARCHAR(5)) AS royalty_fee_receive_address -- we have multiple address so have to null this field
            ,unique_trade_id
        FROM fee_events
            where NOT is_protocol_fee
        GROUP BY block_number,unique_trade_id
    ),


    trades_with_fees AS (
        SELECT
            t.*
            , pf.platform_fee_amount_raw
            , pf.platform_fee_percentage
            , rf.royalty_fee_amount_raw
            , rf.royalty_fee_percentage
            , rf.royalty_fee_receive_address
        FROM trades_with_nft_and_tx t
        LEFT JOIN platform_fees pf
            ON t.block_number = pf.block_number AND t.unique_trade_id = pf.unique_trade_id
        LEFT JOIN royalty_fees rf
            ON t.block_number = rf.block_number AND t.unique_trade_id = rf.unique_trade_id

    ),

    trades_with_price AS (
        SELECT
            t.*
            , p.symbol AS currency_symbol
            , amount_raw / pow(10, p.decimals) AS amount_original
            , amount_raw / pow(10, p.decimals)*p.price AS amount_usd
            , platform_fee_amount_raw / pow(10, p.decimals) AS platform_fee_amount
            , platform_fee_amount_raw / pow(10, p.decimals)*p.price AS platform_fee_amount_usd
            , royalty_fee_amount_raw / pow(10, p.decimals) AS royalty_fee_amount
            , royalty_fee_amount_raw / pow(10, p.decimals)*p.price AS royalty_fee_amount_usd
            , p.symbol AS royalty_fee_currency_symbol
        FROM trades_with_fees t
        LEFT JOIN {{ source('prices', 'usd') }} p ON p.blockchain='ethereum'
            AND p.symbol = 'WETH' -- currently we only have ETH trades
            AND date_trunc('minute', p.minute)=date_trunc('minute', t.block_time)
            {% if is_incremental() %}
            AND p.minute >= date_trunc("day", now() - INTERVAL '1 week')
            {% endif %}
            {% if NOT is_incremental() %}
            AND p.minute >= '2022-4-1'
            {% endif %}
    ),

    trades_enhanced AS (
        SELECT
            t.*
            , nft.standard AS token_standard
            , nft.name AS collection
            , agg.contract_address AS aggregator_address
            , agg.name AS aggregator_name
        FROM trades_with_price t
        LEFT JOIN tokens_ethereum_nft nft
            ON nft_contract_address = nft.contract_address
        LEFT JOIN nft_ethereum_aggregators agg
            ON tx_to = agg.contract_address
    )


SELECT
    'ethereum' AS blockchain
    , 'archipelago' AS project
    , 'v1' AS version
    , TRY_CAST(date_trunc('DAY', te.block_time) AS date) AS block_date
    , te.block_time
    , te.block_number
    , te.token_id
    , te.token_standard
    , 1 AS number_of_items
    , 'Single Item Trade' AS trade_type
    , CASE WHEN te.tx_from = COALESCE(seller_fix.from, te.seller) THEN 'Offer Accepted' ELSE 'Buy' END AS trade_category
    , 'Trade' AS evt_type
    , COALESCE(seller_fix.from, te.seller) AS seller
    , COALESCE(buyer_fix.to, te.buyer) AS buyer
    , te.amount_raw
    , te.amount_original
    , te.amount_usd
    , te.currency_symbol
    , te.currency_contract
    , te.project_contract_address
    , te.nft_contract_address
    , te.collection
    , te.tx_hash
    , te.tx_from
    , te.tx_to
    , te.aggregator_address
    , te.aggregator_name
    , te.platform_fee_amount
    , te.platform_fee_amount_raw
    , te.platform_fee_amount_usd
    , CAST(te.platform_fee_percentage AS DOUBLE) AS platform_fee_percentage
    , te.royalty_fee_amount
    , te.royalty_fee_amount_usd
    , te.royalty_fee_amount_raw
    , te.royalty_fee_currency_symbol
    , te.royalty_fee_receive_address -- NULL here
    , CAST(te.royalty_fee_percentage AS DOUBLE) AS royalty_fee_percentage
    , te.unique_trade_id
FROM trades_enhanced te
LEFT JOIN {{ ref('nft_ethereum_transfers') }} buyer_fix ON buyer_fix.block_time=te.block_time
    AND te.nft_contract_address=buyer_fix.contract_address
    AND buyer_fix.tx_hash=te.tx_hash
    AND te.token_id=buyer_fix.token_id
    AND te.buyer=te.aggregator_address
    AND buyer_fix.from=te.aggregator_address
    {% if is_incremental() %}
    AND buyer_fix.block_time >= date_trunc("day", now() - INTERVAL '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_ethereum_transfers') }} seller_fix ON seller_fix.block_time=te.block_time
    AND te.nft_contract_address=seller_fix.contract_address
    AND seller_fix.tx_hash=te.tx_hash
    AND te.token_id=seller_fix.token_id
    AND te.seller=te.aggregator_address
    AND seller_fix.to=te.aggregator_address
    {% if is_incremental() %}
    AND seller_fix.block_time >= date_trunc("day", now() - INTERVAL '1 week')
    {% endif %}
