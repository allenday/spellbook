{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum","solana", "optimism", "polygon"]\',
                                            "project",
                                            "opensea",
                                            \'["soispoke"]\') }}'
        )
}}

SELECT
    blockchain,
    project,
    version,
    block_time,
    token_id,
    collection,
    amount_usd,
    token_standard,
    trade_type,
    CAST(number_of_items AS DECIMAL(38, 0)) AS number_of_items,
    trade_category,
    evt_type,
    seller,
    buyer,
    amount_original,
    CAST(amount_raw AS DECIMAL(38, 0)) AS amount_raw,
    currency_symbol,
    currency_contract,
    nft_contract_address,
    project_contract_address,
    aggregator_name,
    aggregator_address,
    block_number,
    tx_hash,
    tx_from,
    tx_to,
    unique_trade_id
FROM {{ ref('opensea_ethereum_trades') }}

UNION ALL

SELECT
    blockchain,
    project,
    version,
    block_time,
    CAST(NULL AS VARCHAR(5)) AS token_id,
    CAST(NULL AS VARCHAR(5)) AS collection,
    amount_usd,
    token_standard,
    CAST(NULL AS VARCHAR(5)) AS trade_type,
    CAST(NULL AS DECIMAL(38, 0)) AS number_of_items,
    CAST(NULL AS VARCHAR(5)) AS trade_category,
    evt_type,
    CAST(NULL AS VARCHAR(5)) AS seller,
    CAST(NULL AS VARCHAR(5)) AS buyer,
    amount_original,
    CAST(amount_raw AS DECIMAL(38, 0)) AS amount_raw,
    currency_symbol,
    currency_contract,
    CAST(NULL AS VARCHAR(5)) AS nft_contract_address,
    project_contract_address,
    CAST(NULL AS VARCHAR(5)) AS aggregator_name,
    CAST(NULL AS VARCHAR(5)) AS aggregator_address,
    CAST(block_number AS BIGINT) AS block_number,
    tx_hash,
    CAST(NULL AS VARCHAR(5)) AS tx_from,
    CAST(NULL AS VARCHAR(5)) AS tx_to,
    unique_trade_id
FROM {{ ref('opensea_solana_trades') }}

UNION ALL

SELECT
    blockchain,
    project,
    version,
    block_time,
    token_id,
    collection,
    amount_usd,
    token_standard,
    trade_type,
    number_of_items,
    trade_category,
    evt_type,
    seller,
    buyer,
    amount_original,
    amount_raw,
    currency_symbol,
    currency_contract,
    nft_contract_address,
    project_contract_address,
    aggregator_name,
    aggregator_address,
    block_number,
    tx_hash,
    tx_from,
    tx_to,
    unique_trade_id
FROM {{ ref('opensea_polygon_trades') }}

UNION ALL

SELECT
    blockchain,
    project,
    version,
    block_time,
    token_id,
    collection,
    amount_usd,
    token_standard,
    trade_type,
    CAST(number_of_items AS DECIMAL(38, 0)) AS number_of_items,
    trade_category,
    evt_type,
    seller,
    buyer,
    amount_original,
    CAST(amount_raw AS DECIMAL(38, 0)) AS amount_raw,
    currency_symbol,
    currency_contract,
    nft_contract_address,
    project_contract_address,
    aggregator_name,
    aggregator_address,
    block_number,
    tx_hash,
    tx_from,
    tx_to,
    unique_trade_id
FROM {{ ref('opensea_optimism_trades') }}
