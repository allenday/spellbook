{{ config( alias='native', tags=['static'])}}

SELECT chain, symbol, price_symbol, LOWER(price_address) as price_address, decimals
FROM UNNEST(ARRAY<STRUCT<chain STRING,symbol STRING,price_symbol STRING,price_address STRING,decimals INT64>> [STRUCT('ethereum', 'ETH', 'WETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 18)])