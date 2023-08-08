{{ config(
      alias='interest'
      
  )
}}

SELECT *
FROM 
(
      SELECT
            reserve,
            symbol,
            `hour`,
            deposit_apy,
            stable_borrow_apy,
            variable_borrow_apy
      FROM {{ ref('aave_v2_ethereum_interest_rates') }}
      /*
      UNION ALL
      < add new version as needed
      */
)