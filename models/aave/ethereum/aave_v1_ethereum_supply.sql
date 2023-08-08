{{ config(
    schema = 'aave_v1_ethereum'
    , alias='supply'
    
  )
}}

{% set aave_mock_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}
{% set weth_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}

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
    '1' AS version,
    'deposit' AS transaction_type,
    CASE
        WHEN CAST(_reserve AS STRING) = '{{aave_mock_address}}' THEN '{{weth_address}}' --Using WETH instead of Aave "mock" address
        ELSE CAST(_reserve AS STRING)
    END AS token,
    _user AS depositor, 
    CAST(NULL AS STRING) AS withdrawn_to,
    CAST(NULL AS STRING) AS liquidator,
    CAST(_amount AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_ethereum','LendingPool_evt_Deposit') }}
UNION ALL 
SELECT 
    '1' AS version,
    'withdraw' AS transaction_type,
    CASE
        WHEN CAST(_reserve AS STRING) = '{{aave_mock_address}}' THEN '{{weth_address}}' --Using WETH instead of Aave "mock" address
        ELSE CAST(_reserve AS STRING)
    END AS token,
    _user AS depositor,
    CAST(_user AS STRING) AS withdrawn_to,
    CAST(NULL AS STRING) AS liquidator,
    - CAST(_amount AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_ethereum','LendingPool_evt_RedeemUnderlying') }}
UNION ALL 
SELECT 
    '1' AS version,
    'deposit_liquidation' AS transaction_type,
    CASE
        WHEN CAST(_collateral AS STRING) = '{{aave_mock_address}}' THEN '{{weth_address}}' --Using WETH instead of Aave "mock" address
        ELSE CAST(_collateral AS STRING)
    END AS token,
    _user AS depositor,
    CAST(_liquidator AS STRING) AS withdrawn_to,
    CAST(_liquidator AS STRING) AS liquidator,
    - CAST(_liquidatedCollateralAmount AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_ethereum','LendingPool_evt_LiquidationCall') }}
) deposit
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20
    ON deposit.token = erc20.contract_address
LEFT JOIN {{ source('prices','usd') }} p 
    ON p.minute = TIMESTAMP_TRUNC(deposit.evt_block_time, minute) 
    AND CAST(p.contract_address AS STRING) = deposit.token 
    AND p.blockchain = 'ethereum'