{{ config(
    schema = 'element_bnb',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id']
    )
}}

WITH element_txs AS (
    -- BNB ERC721 Sells
    SELECT
        'bnb' AS blockchain,
        'element' AS project,
        'v1' AS version,
        ee.evt_block_time AS block_time,
        ee.erc721tokenid AS token_id,
        'erc721' AS token_standard,
        'Single Item Trade' AS trade_type,
        1 AS number_of_items,
        'Offer Accepted' AS trade_category,
        ee.maker AS seller,
        ee.taker AS buyer,
        ee.erc20tokenamount AS amount_raw,
        CASE
            WHEN ee.erc20token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
            ELSE ee.erc20token
        END AS currency_contract,
        CASE WHEN ee.erc20token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'BNB' END AS currency_symbol,
        ee.erc721token AS nft_contract_address,
        ee.contract_address AS project_contract_address,
        ee.evt_tx_hash AS tx_hash,
        ee.evt_block_number AS block_number
    FROM {{ source('element_ex_bnb','OrdersFeature_evt_ERC721SellOrderFilled') }} AS ee
    {% if is_incremental() %}
        WHERE ee.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    UNION ALL

    -- BNB ERC721 Buys
    SELECT
        'bnb' AS blockchain,
        'element' AS project,
        'v1' AS version,
        ee.evt_block_time AS block_time,
        ee.erc721tokenid AS token_id,
        'erc721' AS token_standard,
        'Single Item Trade' AS trade_type,
        1 AS number_of_items,
        'Buy' AS trade_category,
        ee.taker AS seller,
        ee.maker AS buyer,
        ee.erc20tokenamount AS amount_raw,
        CASE
            WHEN ee.erc20token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
            ELSE ee.erc20token
        END AS currency_contract,
        CASE WHEN ee.erc20token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'BNB' END AS currency_symbol,
        ee.erc721token AS nft_contract_address,
        ee.contract_address AS project_contract_address,
        ee.evt_tx_hash AS tx_hash,
        ee.evt_block_number AS block_number
    FROM {{ source('element_ex_bnb','OrdersFeature_evt_ERC721BuyOrderFilled') }} AS ee
    {% if is_incremental() %}
        WHERE ee.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    UNION ALL

    -- BNB ERC1155 Sells
    SELECT
        'bnb' AS blockchain,
        'element' AS project,
        'v1' AS version,
        ee.evt_block_time AS block_time,
        ee.erc1155tokenid AS token_id,
        'erc1155' AS token_standard,
        'Single Item Trade' AS trade_type,
        1 AS number_of_items,
        'Offer Accepted' AS trade_category,
        ee.maker AS seller,
        ee.taker AS buyer,
        ee.erc20fillamount AS amount_raw,
        CASE
            WHEN ee.erc20token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
            ELSE ee.erc20token
        END AS currency_contract,
        CASE WHEN ee.erc20token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'BNB' END AS currency_symbol,
        ee.erc1155token AS nft_contract_address,
        ee.contract_address AS project_contract_address,
        ee.evt_tx_hash AS tx_hash,
        ee.evt_block_number AS block_number
    FROM {{ source('element_ex_bnb','OrdersFeature_evt_ERC1155SellOrderFilled') }} AS ee
    {% if is_incremental() %}
        WHERE ee.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    UNION ALL

    -- BNB ERC1155 Buys
    SELECT
        'bnb' AS blockchain,
        'element' AS project,
        'v1' AS version,
        ee.evt_block_time AS block_time,
        ee.erc1155tokenid AS token_id,
        'erc1155' AS token_standard,
        'Single Item Trade' AS trade_type,
        1 AS number_of_items,
        'Buy' AS trade_category,
        ee.taker AS seller,
        ee.maker AS buyer,
        ee.erc20fillamount AS amount_raw,
        CASE
            WHEN ee.erc20token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
            ELSE ee.erc20token
        END AS currency_contract,
        CASE WHEN ee.erc20token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'BNB' END AS currency_symbol,
        ee.erc1155token AS nft_contract_address,
        ee.contract_address AS project_contract_address,
        ee.evt_tx_hash AS tx_hash,
        ee.evt_block_number AS block_number
    FROM {{ source('element_ex_bnb','OrdersFeature_evt_ERC1155BuyOrderFilled') }} AS ee
    {% if is_incremental() %}
        WHERE ee.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
)

SELECT
    alet.blockchain,
    alet.project,
    alet.version,
    alet.block_time,
    date_trunc('day', alet.block_time) AS block_date,
    alet.token_id,
    bnb_nft_tokens.name AS collection,
    alet.amount_raw / POWER(10, bnb_bep20_tokens.decimals) * prices.price AS amount_usd,
    alet.token_standard,
    CASE WHEN agg.name IS NOT NULL THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type,
    CAST(alet.number_of_items AS decimal(38, 0)) AS number_of_items,
    alet.trade_category,
    'Trade' AS evt_type,
    alet.seller,
    alet.buyer,
    alet.amount_raw / POWER(10, bnb_bep20_tokens.decimals) AS amount_original,
    CAST(alet.amount_raw AS decimal(38, 0)) AS amount_raw,
    COALESCE(alet.currency_symbol, bnb_bep20_tokens.symbol) AS currency_symbol,
    alet.currency_contract,
    alet.nft_contract_address,
    alet.project_contract_address,
    agg.name AS aggregator_name,
    CASE WHEN agg.name IS NOT NULL THEN agg.contract_address END AS aggregator_address,
    alet.tx_hash,
    alet.block_number,
    bt.from AS tx_from,
    bt.to AS tx_to,
    CAST(0 AS double) AS platform_fee_amount_raw,
    CAST(0 AS double) AS platform_fee_amount,
    CAST(0 AS double) AS platform_fee_amount_usd,
    CAST(0 AS double) AS platform_fee_percentage,
    CAST(0 AS double) AS royalty_fee_amount_raw,
    CAST(0 AS double) AS royalty_fee_amount,
    CAST(0 AS double) AS royalty_fee_amount_usd,
    CAST(0 AS double) AS royalty_fee_percentage,
    CAST('0' AS varchar(5)) AS royalty_fee_receive_address,
    CAST('0' AS varchar(5)) AS royalty_fee_currency_symbol,
    alet.blockchain || alet.project || alet.version || alet.tx_hash || alet.seller || alet.buyer || alet.nft_contract_address || alet.token_id AS unique_trade_id
FROM element_txs AS alet
LEFT JOIN {{ ref('nft_aggregators') }} AS agg ON alet.buyer = agg.contract_address AND agg.blockchain = 'bnb'
LEFT JOIN {{ ref('tokens_erc20') }} AS bnb_bep20_tokens ON bnb_bep20_tokens.contract_address = alet.currency_contract AND bnb_bep20_tokens.blockchain = 'bnb'
LEFT JOIN {{ ref('tokens_nft') }} AS bnb_nft_tokens ON bnb_nft_tokens.contract_address = alet.currency_contract AND bnb_nft_tokens.blockchain = 'bnb'
LEFT JOIN {{ source('prices', 'usd') }} AS prices
    ON
        prices.minute = date_trunc('minute', alet.block_time)
        AND (prices.contract_address = alet.currency_contract AND prices.blockchain = alet.blockchain)
        {% if is_incremental() %}
            AND prices.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN {{ source('bnb','transactions') }} AS bt
    ON
        bt.hash = alet.tx_hash
        AND bt.block_time = alet.block_time
        {% if is_incremental() %}
            AND bt.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
