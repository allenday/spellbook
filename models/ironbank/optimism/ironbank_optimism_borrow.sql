{{ config(
    alias = 'borrow'
) }}

SELECT
b.evt_block_number AS block_number,
b.evt_block_time AS block_time,
b.evt_tx_hash AS tx_hash,
b.evt_index AS `index`,
CAST(b.contract_address AS STRING) AS contract_address,
b.borrower,
i.symbol,
i.underlying_symbol,
i.underlying_token_address AS underlying_address,
CAST(b.borrowAmount AS FLOAT64) / power(10,i.underlying_decimals) AS borrow_amount,
CAST(b.borrowAmount AS FLOAT64) / power(10,i.underlying_decimals)*p.price AS borrow_usd
FROM {{ source('ironbank_optimism', 'CErc20Delegator_evt_Borrow') }} b
LEFT JOIN {{ ref('ironbank_optimism_itokens') }} i ON CAST(b.contract_address AS STRING) = i.contract_address
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = TIMESTAMP_TRUNC(b.evt_block_time, minute) AND CAST(p.contract_address AS STRING) = i.underlying_token_address AND p.blockchain = 'optimism'