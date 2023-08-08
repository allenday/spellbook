{{ config(
    schema = 'aave_v3_optimism'
    , materialized = 'view'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['version', 'token_address', 'evt_tx_hash', 'evt_block_number', 'evt_index']
    , alias='borrow'
    
  )
}}

SELECT
      version,
      transaction_type,
      loan_type,
      erc20.symbol,
      borrow.token as token_address,
      borrower,
      repayer,
      liquidator,
      amount / CAST(CONCAT('1e',CAST(erc20.decimals AS STRING)) AS FLOAT64) AS amount,
      (amount/ CAST(CONCAT('1e',CAST(p.decimals AS STRING)) AS FLOAT64)) * price AS usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number   
FROM (
SELECT 
    '3' AS version,
    'borrow' AS transaction_type,
    CASE 
        WHEN interestRateMode = 1 THEN 'stable'
        WHEN interestRateMode = 2 THEN 'variable'
    END AS loan_type,
    CAST(reserve AS STRING) AS token,
    user AS borrower, 
    CAST(NULL AS STRING) AS repayer,
    CAST(NULL AS STRING) AS liquidator,
    CAST(amount AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_Borrow') }} 
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', CURRENT_TIMESTAMP() - interval '1 week')
{% endif %}
UNION ALL 
SELECT 
    '3' AS version,
    'repay' AS transaction_type,
    NULL AS loan_type,
    CAST(reserve AS STRING) AS token,
    user AS borrower,
    CAST(repayer AS STRING) AS repayer,
    CAST(NULL AS STRING) AS liquidator,
    - CAST(amount AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_Repay') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', CURRENT_TIMESTAMP() - interval '1 week')
{% endif %}
UNION ALL
SELECT 
    '3' AS version,
    'borrow_liquidation' AS transaction_type,
    NULL AS loan_type,
    CAST(debtAsset AS STRING) AS token,
    user AS borrower,
    CAST(liquidator AS STRING) AS repayer,
    CAST(liquidator AS STRING) AS liquidator,
    - CAST(debtToCover AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_LiquidationCall') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', CURRENT_TIMESTAMP() - interval '1 week')
{% endif %}
) borrow
LEFT JOIN {{ ref('tokens_optimism_erc20') }} erc20
    ON borrow.token = erc20.contract_address
LEFT JOIN {{ source('prices','usd') }} p 
    ON p.minute = TIMESTAMP_TRUNC(borrow.evt_block_time, minute) 
    AND p.symbol = erc20.symbol 
    AND p.contract_address = borrow.token
    AND p.blockchain = 'optimism'
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}