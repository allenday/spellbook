{{ config(
    schema = 'aave_v2_ethereum'
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
    '2' AS version,
    'borrow' AS transaction_type,
    CASE 
        WHEN CAST(borrowRateMode AS STRING) = '1' THEN 'stable'
        WHEN CAST(borrowRateMode AS STRING) = '2' THEN 'variable'
    END AS loan_type,
    CAST(reserve AS STRING) AS token,
    CAST(user AS STRING) AS borrower, 
    CAST(NULL AS STRING) AS repayer,
    CAST(NULL AS STRING) AS liquidator,
    CAST(amount AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v2_ethereum','LendingPool_evt_Borrow') }} 
UNION ALL 
SELECT 
    '2' AS version,
    'repay' AS transaction_type,
    NULL AS loan_type,
    CAST(reserve AS STRING) AS token,
    CAST(user AS STRING) AS borrower,
    CAST(repayer AS STRING) AS repayer,
    CAST(NULL AS STRING) AS liquidator,
    - CAST(amount AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v2_ethereum','LendingPool_evt_Repay') }}
UNION ALL
SELECT 
    '2' AS version,
    'borrow_liquidation' AS transaction_type,
    NULL AS loan_type,
    CAST(debtAsset AS STRING) AS token,
    CAST(user AS STRING) AS borrower,
    CAST(liquidator AS STRING) AS repayer,
    CAST(liquidator AS STRING)  AS liquidator,
    - CAST(debtToCover AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v2_ethereum','LendingPool_evt_LiquidationCall') }}
) borrow
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20
    ON borrow.token = erc20.contract_address
LEFT JOIN {{ source('prices','usd') }} p 
    ON p.minute = TIMESTAMP_TRUNC(borrow.evt_block_time, minute) 
    AND CAST(p.contract_address AS STRING) = borrow.token
    AND p.blockchain = 'ethereum'