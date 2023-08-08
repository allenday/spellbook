 {{ config( alias='aggregators') }}

SELECT
  contract_address,
  name
FROM UNNEST(ARRAY<STRUCT<contract_address STRING,name STRING>> [STRUCT('0x56085ea9c43dea3c994c304c53b9915bff132d20', 'Element') , ('0x48939e2b2549710df8b7d9085207279a8f0fe3e5', 'Oxalus') , ('0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad', 'Uniswap')])