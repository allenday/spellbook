{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "nftrade",
                                \'["Henrystats"]\') }}'
    )
}}

{%- set project_start_date = '2022-09-17' %}

WITH

-- fill events
source_inventory AS (
    SELECT
        contract_address,
        evt_block_number,
        evt_block_time,
        evt_index,
        evt_tx_hash,
        makeraddress AS maker_address,
        makerassetamount AS maker_asset_amount_raw,
        makerassetamount / POW(10, 18) AS maker_asset_amount,
        CONCAT('0x', SUBSTRING(makerassetdata, 35, 40)) AS maker_asset_address,
        CAST(bytea2numeric_v3(SUBSTRING(makerassetdata, 77, 64)) AS decimal(38)) AS maker_id,
        marketplaceidentifier AS marketplace_identifier,
        protocolfeepaid * 1 AS protocol_fees_raw,
        protocolfeepaid / POW(10, 18) AS protocol_fees,
        royaltiesaddress AS royalties_address,
        royaltiesamount AS royalty_fees_raw,
        royaltiesamount / POW(10, 18) AS royalty_fees,
        senderaddress AS sender_address,
        takeraddress AS taker_address,
        takerassetamount AS taker_asset_amount_raw,
        takerassetamount / POW(10, 18) AS taker_asset_amount,
        CONCAT('0x', SUBSTRING(takerassetdata, 35, 40)) AS taker_asset_address,
        CAST(bytea2numeric_v3(SUBSTRING(takerassetdata, 77, 64)) AS decimal(38)) AS taker_id
    FROM
        {{ source('nftrade_bnb', 'NiftyProtocol_evt_Fill') }}
    WHERE
        evt_block_time >= '{{ project_start_date }}'
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
),

source_inventory_enriched AS (
    SELECT
        src.*,
        CASE
            WHEN src.maker_id = 0 OR src.maker_id IS NULL THEN taker_asset_address
            ELSE maker_asset_address
        END AS nft_contract_address,
        CAST((CASE
            WHEN src.maker_id = 0 OR src.maker_id IS NULL THEN src.taker_id
            ELSE src.maker_id
        END) AS varchar(100)) AS token_id,
        CAST((CASE
            WHEN src.maker_id = 0 OR src.maker_id IS NULL THEN src.maker_asset_amount_raw
            ELSE src.taker_asset_amount_raw
        END) AS decimal(38, 0)) AS amount_raw,
        CASE
            WHEN src.maker_id = 0 OR src.maker_id IS NULL THEN src.maker_asset_amount
            ELSE src.taker_asset_amount
        END AS amount_original,
        CASE
            WHEN src.maker_id = 0 OR src.maker_id IS NULL THEN 'Sell'
            ELSE 'Buy'
        END AS trade_category,
        CASE
            WHEN src.maker_id = 0 OR src.maker_id IS NULL THEN src.maker_address
            ELSE src.taker_address
        END AS buyer,
        CASE
            WHEN src.maker_id = 0 OR src.maker_id IS NULL THEN src.taker_address
            ELSE src.maker_address
        END AS seller,
        CASE
            WHEN src.maker_id = 0 OR src.maker_id IS NULL THEN (src.protocol_fees / src.maker_asset_amount) * 100
            ELSE (src.protocol_fees / src.taker_asset_amount) * 100
        END AS platform_fee_percentage,
        CASE
            WHEN src.maker_id = 0 OR src.maker_id IS NULL THEN (src.royalty_fees / src.maker_asset_amount) * 100
            ELSE (src.royalty_fees / src.taker_asset_amount) * 100
        END AS royalty_fee_percentage
    FROM
        source_inventory AS src
)

SELECT
    'bnb' AS blockchain,
    'nftrade' AS project,
    'v1' AS version,
    src.evt_block_time AS block_time,
    date_trunc('day', src.evt_block_time) AS block_date,
    src.evt_block_number AS block_number,
    src.token_id,
    nft_token.name AS collection,
    CAST(src.amount_raw AS decimal(38, 0)) AS amount_raw,
    src.amount_original,
    src.amount_original * p.price AS amount_usd,
    CASE
        WHEN erc721.evt_index IS NOT NULL THEN 'erc721'
        ELSE 'erc1155'
    END AS token_standard,
    'Single Item Trade' AS trade_type,
    CAST(1 AS decimal(38, 0)) AS number_of_items,
    src.trade_category,
    'Trade' AS evt_type,
    src.buyer,
    src.seller,
    'BNB' AS currency_symbol,
    LOWER('0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c') AS currency_contract,
    src.nft_contract_address,
    src.contract_address AS project_contract_address,
    agg.name AS aggregator_name,
    agg.contract_address AS aggregator_address,
    src.evt_tx_hash AS tx_hash,
    btx.from AS tx_from,
    btx.to AS tx_to,
    CAST(src.protocol_fees_raw AS double) AS platform_fee_amount_raw,
    CAST(src.protocol_fees AS double) AS platform_fee_amount,
    CAST(src.protocol_fees * p.price AS double) AS platform_fee_amount_usd,
    CAST(src.platform_fee_percentage AS double) AS platform_fee_percentage,
    CAST(src.royalty_fees_raw AS double) AS royalty_fee_amount_raw,
    src.royalty_fees AS royalty_fee_amount,
    src.royalty_fees * p.price AS royalty_fee_amount_usd,
    CAST(src.royalty_fee_percentage AS double) AS royalty_fee_percentage,
    'BNB' AS royalty_fee_currency_symbol,
    royalties_address AS royalty_fee_receive_address,
    src.evt_index,
    'bnb-nftrade-v1' || '-' || src.evt_block_number || '-' || src.evt_tx_hash || '-' || src.evt_index AS unique_trade_id
FROM
    source_inventory_enriched AS src
INNER JOIN
    {{ source('bnb','transactions') }} AS btx
    ON
        btx.block_time = src.evt_block_time
        AND btx.hash = src.evt_tx_hash
        {% if not is_incremental() %}
        AND btx.block_time >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
            AND btx.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN
    {{ ref('tokens_bnb_nft') }} AS nft_token
    ON nft_token.contract_address = src.nft_contract_address
LEFT JOIN
    {{ source('prices','usd') }} AS p
    ON
        p.blockchain = 'bnb'
        AND p.minute = date_trunc('minute', src.evt_block_time)
        AND p.contract_address = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
        {% if not is_incremental() %}
        AND p.minute >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
            AND p.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN
    {{ ref('nft_bnb_aggregators') }} AS agg
    ON agg.contract_address = src.sender_address
LEFT JOIN
    {{ source('erc721_bnb','evt_transfer') }} AS erc721
    ON
        erc721.evt_block_time = src.evt_block_time
        AND erc721.evt_tx_hash = src.evt_tx_hash
        AND erc721.contract_address = src.nft_contract_address
        AND erc721.tokenid = src.token_id
        AND erc721.to = src.buyer
        {% if not is_incremental() %}
        AND erc721.evt_block_time >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
            AND erc721.evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
