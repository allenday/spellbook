 {{ config( alias='aggregators') }}

SELECT
  contract_address,
  name
FROM UNNEST(ARRAY<STRUCT<contract_address STRING,name BIGNUMERIC>> [STRUCT('0x37ad2bd1e4f1c0109133e07955488491233c9372', 'Element')])