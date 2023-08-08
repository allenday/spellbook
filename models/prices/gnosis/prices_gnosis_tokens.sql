{{ config(
        schema='prices_gnosis',
        alias ='tokens',
        materialized = 'view',
                tags=['static']
        )
}}
SELECT 
    TRIM(token_id) as token_id
    , LOWER(TRIM(blockchain)) as blockchain
    , TRIM(symbol) as symbol
    , LOWER(TRIM(contract_address)) as contract_address
    , decimals
FROM UNNEST(ARRAY<STRUCT<token_id STRING,blockchain STRING,symbol STRING,contract_address STRING,decimals INT64>> [STRUCT("dai-dai", "gnosis", "WXDAI", "0xe91d153e0b41518a2ce8dd3d7944fa863463a97d", 18),
STRUCT("usdc-usd-coin", "gnosis", "USDC", "0xddafbb505ad214d7b80b1f830fccc89b60fb7a83", 6),
STRUCT("usdt-tether", "gnosis", "USDT", "0x4ecaba5870353805a9f068101a40e0f32ed605c6", 6),
STRUCT("wbtc-wrapped-bitcoin", "gnosis", "WBTC", "0x8e5bbbb09ed1ebde8674cda39a0c169401db4252", 8),
STRUCT("weth-weth", "gnosis", "WETH", "0x6a023ccd1ff6f2045c3309768ead9e68f978f6e1", 18),
STRUCT("gno-gnosis", "gnosis", "GNO", "0x9c58bacc331c9aa871afd802db6379a98e80cedb", 18),
STRUCT("cow-cow-protocol-token", "gnosis", "COW", "0x177127622c4a00f3d409b75571e12cb3c8973d3c", 18),
STRUCT("matic-polygon", "gnosis", "MATIC", "0x7122d7661c4564b7c6cd4878b06766489a6028a2", 18),
STRUCT("1inch-1inch", "gnosis", "1INCH", "0x7f7440c5098462f833e123b44b8a03e1d9785bab", 18),
STRUCT("dai-dai", "gnosis", "DAI", "0x44fa8e6f47987339850636f88629646662444217", 18),
STRUCT("busd-binance-usd", "gnosis", "BUSD", "0xdd96b45877d0e8361a4ddb732da741e97f3191ff", 18),
STRUCT("crv-curve-dao-token", "gnosis", "CRV", "0x712b3d230f3c1c19db860d80619288b1f0bdd0bd", 18),
STRUCT("link-chainlink", "gnosis", "LINK", "0xe2e73a1c69ecf83f464efce6a5be353a37ca09b2", 18),
STRUCT("sushi-sushi", "gnosis", "SUSHI", "0x2995d1317dcd4f0ab89f4ae60f3f020a4f17c7ce", 18)])