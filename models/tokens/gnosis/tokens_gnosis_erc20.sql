{{ config( alias='erc20', tags=['static'])}}

SELECT LOWER(contract_address) as contract_address, symbol, decimals
FROM
  {{ ref( 'tokens_gnosis_erc20_0_seed' ) }}