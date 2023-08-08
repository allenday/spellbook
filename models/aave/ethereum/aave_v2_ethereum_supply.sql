{{ config(
    schema = 'aave_v2_ethereum'
    , alias='supply'
    
  )
}}

SELECT 
      version,
      transaction_type,
      erc20.symbol,
      deposit.token as token_address, 
      depositor,
      withdrawn_to,
      liquidator,
      amount / CAST(CONCAT('1e',CAST(erc20.decimals AS STRING)) AS FLOAT64) AS amount,
      (amount / CAST(CONCAT('1e',CAST(p.decimals AS STRING)) AS FLOAT64)) * price AS usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number 
FROM (
SELECT 
    '2' AS version,
    'deposit' AS transaction_type,
    CAST(reserve AS STRING) AS token,
    user AS depositor, 
    CAST(NULL AS STRING) as withdrawn_to,
    CAST(NULL AS STRING) AS liquidator,
    CAST(amount AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v2_ethereum','LendingPool_evt_Deposit') }}
UNION ALL 
SELECT 
    '2' AS version,
    'withdraw' AS transaction_type,
    CAST(reserve AS STRING) AS token,
    user AS depositor,
    CAST(`to` AS STRING) AS withdrawn_to,
    CAST(NULL AS STRING) AS liquidator,
    - CAST(amount AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v2_ethereum','LendingPool_evt_Withdraw') }}
UNION ALL
SELECT 
    '2' AS version,
    'deposit_liquidation' AS transaction_type,
    CAST(collateralAsset AS STRING) AS token,
    user AS depositor,
    CAST(liquidator AS STRING) AS withdrawn_to,
    CAST(liquidator AS STRING) AS liquidator,
    - CAST(liquidatedCollateralAmount AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v2_ethereum','LendingPool_evt_LiquidationCall') }}
) deposit
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20
    ON deposit.token = erc20.contract_address
LEFT JOIN {{ source('prices','usd') }} p 
    ON p.minute = TIMESTAMP_TRUNC(deposit.evt_block_time, minute) 
    AND CAST(p.contract_address AS STRING) = deposit.token 
    AND p.blockchain = 'ethereum'