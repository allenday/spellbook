{{ config(
        alias ='trades'
        )
}}
 
SELECT blockchain,
project,
version,
block_time,
token_id,
CAST(NULL AS STRING) as collection,
amount_usd,
token_standard,
CAST(NULL AS STRING) as trade_type,
CAST(number_of_items AS BIGNUMERIC) AS number_of_items,
CAST(NULL AS STRING) as trade_category,
evt_type,
seller,
buyer,
amount_original,
CAST(amount_raw AS BIGNUMERIC) AS amount_raw,
currency_symbol,
currency_contract,
CAST(NULL AS STRING) as nft_contract_address,
project_contract_address,
CAST(NULL AS STRING) as aggregator_name,
CAST(NULL AS STRING) as aggregator_address,
CAST(block_number AS BIGINT) as block_number,
tx_hash,
CAST(NULL AS STRING) as tx_from,
CAST(NULL AS STRING) as tx_to,
unique_trade_id
FROM {{ ref('magiceden_solana_trades') }}

UNION ALL
 
SELECT blockchain,
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
FROM {{ ref('magiceden_polygon_trades') }}