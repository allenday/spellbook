{{ config(alias='safe_airdrop', materialized = 'view',         tags=['static']) }}

SELECT address, token_amount
FROM
  {{ ref( 'addresses_ethereum_safe_airdrop_0_seed' ) }}