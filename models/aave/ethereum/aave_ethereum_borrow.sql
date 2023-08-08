{{ config(
      alias='borrow'
      
  )
}}

SELECT *
FROM 
(
      SELECT
            version,
            transaction_type,
            loan_type,
            symbol,
            token_address,
            borrower,
            repayer,
            liquidator,
            amount,
            usd_amount,
            evt_tx_hash,
            evt_index,
            evt_block_time,
            evt_block_number  
      FROM {{ ref('aave_v1_ethereum_borrow') }}
      UNION ALL
      SELECT
            version,
            transaction_type,
            loan_type,
            symbol,
            token_address,
            borrower,
            repayer,
            liquidator,
            amount,
            usd_amount,
            evt_tx_hash,
            evt_index,
            evt_block_time,
            evt_block_number  
      FROM {{ ref('aave_v2_ethereum_borrow') }} 
)