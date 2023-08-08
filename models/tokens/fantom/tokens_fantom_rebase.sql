{{ config( alias='rebase', tags=['static'])}}

SELECT LOWER(contract_address) as contract_address, symbol, decimals 
FROM UNNEST(ARRAY<STRUCT<contract_address STRING,symbol STRING,decimals INT64>> [STRUCT('0xfa1FBb8Ef55A4855E5688C0eE13aC3f202486286', 'FHM', 9)])