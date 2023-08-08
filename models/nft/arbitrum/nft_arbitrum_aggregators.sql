{{ config( alias='aggregators') }}

SELECT
  lower(contract_address) as contract_address,
  name
FROM UNNEST(ARRAY<STRUCT<contract_address STRING,name INT64>> [STRUCT('0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad', 'Uniswap')])