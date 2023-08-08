{{ config( alias='bep20', tags=['static'])}}

SELECT LOWER(contract_address) AS contract_address, symbol, decimals
  FROM
  {{ ref( 'tokens_bnb_bep20_0_seed' ) }}