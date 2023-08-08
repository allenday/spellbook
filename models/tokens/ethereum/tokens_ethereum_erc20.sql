{{ config(
        alias='erc20'
        , tags=['static']
        )
}}
SELECT
        LOWER(contract_address) as contract_address
        , trim(symbol) as symbol
        , decimals
FROM
  {{ ref( 'tokens_ethereum_erc20_0_seed' ) }}