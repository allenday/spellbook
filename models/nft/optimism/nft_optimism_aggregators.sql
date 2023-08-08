{{ config( alias='aggregators') }}

SELECT
  lower(contract_address) as contract_address,
  name
FROM UNNEST(ARRAY<STRUCT<contract_address STRING,name STRING>> [STRUCT('0xbbbbbbbe843515689f3182b748b5671665541e58', 'bluesweep') , ('0x92D932aBBC7885999c4347880Eb069F854982eDD', 'okx') , ('0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad', 'Uniswap')])