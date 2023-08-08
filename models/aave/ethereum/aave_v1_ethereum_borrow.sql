{{ config(
    schema = 'aave_v1_ethereum'
    , alias='borrow'
    
  )
}}

{% set aave_mock_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}
{% set weth_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}

SELECT
      version,
      transaction_type,
      loan_type,
      erc20.symbol,
      borrow.token as token_address,
      borrower,
      repayer,
      liquidator,
      amount / CAST(CONCAT('1e', CAST(erc20.decimals AS STRING)) AS FLOAT64) AS amount,
      (amount/ CAST(CONCAT('1e', CAST(p.decimals AS STRING)) AS FLOAT64)) * price AS usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number   
FROM (
SELECT 
    '1' AS version,
    'borrow' AS transaction_type,
    CASE 
        WHEN CAST(_borrowRateMode AS STRING) = '1' THEN 'stable'
        WHEN CAST(_borrowRateMode AS STRING) = '2' THEN 'variable'
    END AS loan_type,
    CASE
        WHEN CAST(_reserve AS STRING) = '{{aave_mock_address}}' THEN '{{weth_address}}' --Using WETH instead of Aave "mock" address
        ELSE CAST(_reserve AS STRING)
    END AS token,
    CAST(_user AS STRING) AS borrower,
    CAST(NULL AS STRING) AS repayer,
    CAST(NULL AS STRING) AS liquidator,
    CAST(_amount AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_ethereum','LendingPool_evt_Borrow') }} 
UNION ALL 
SELECT 
    '1' AS version,
    'repay' AS transaction_type,
    NULL AS loan_type,
    CASE
        WHEN CAST(_reserve AS STRING) = '{{aave_mock_address}}' THEN '{{weth_address}}' --Using WETH instead of Aave "mock" address
        ELSE CAST(_reserve AS STRING)
    END AS token,
    CAST(_user AS STRING) AS borrower,
    CAST(_repayer AS STRING) AS repayer,
    CAST(NULL AS STRING) AS liquidator,
    - CAST(_amountMinusFees AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_ethereum','LendingPool_evt_Repay') }}
UNION ALL
SELECT 
    '1' AS version,
    'borrow_liquidation' AS transaction_type,
    NULL AS loan_type,
    CASE
        WHEN CAST(_reserve AS STRING) = '{{aave_mock_address}}' THEN '{{weth_address}}' --Using WETH instead of Aave "mock" address
        ELSE CAST(_reserve AS STRING)
    END AS token,
    CAST(_user AS STRING) AS borrower,
    CAST(_liquidator AS STRING) AS repayer,
    CAST(_liquidator AS STRING) AS liquidator,
    - CAST(_purchaseAmount AS BIGNUMERIC) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_ethereum','LendingPool_evt_LiquidationCall') }}
) borrow
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20
    ON borrow.token = erc20.contract_address
LEFT JOIN {{ source('prices','usd') }} p 
    ON p.minute = TIMESTAMP_TRUNC(borrow.evt_block_time, minute) 
    AND CAST(p.contract_address AS STRING) = borrow.token
    AND p.blockchain = 'ethereum'