{{ config(
        schema='prices_polygon',
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
FROM UNNEST(ARRAY<STRUCT<token_id STRING,blockchain STRING,symbol STRING,contract_address STRING,decimals INT64>> [STRUCT("aave-new","polygon","AAVE","0xd6df932a45c0f255f85145f286ea0b292b21c90b",18),
STRUCT("ageur-ageur","polygon",'agEUR',"0xe0b52e49357fd4daf2c15e02058dce6bc0057db4",18),
STRUCT("axlusdc-axelar-usd-coin", "polygon", "axlUSDC", "0x750e4c4984a9e0f12978ea6742bc1c5d248f40ed", 6),
STRUCT("axlusdt-axelar-usd-tether", "polygon", "axlUSDT", "0xceed2671d8634e3ee65000edbbee66139b132fbf", 6),
STRUCT("axlatom-axelar-wrapped-atom", "polygon", "axlATOM", "0x33f8a5029264bcfb66e39157af3fea3e2a8a5067", 6),
STRUCT("bal-balancer","polygon","BAL","0x9a71012b13ca4d3d0cdc72a177df3ef03b0e76a3",18),
STRUCT("dpi-defi-pulse-index", "polygon", "DPI", "0x85955046df4668e1dd369d2de9f3aeb98dd2a369", 18),
STRUCT("eurs-stasis-eurs","polygon","EURS","0xe111178a87a3bff0c8d18decba5798827539ae99",2),
STRUCT("matic-polygon", "polygon", "MATIC", "0x0000000000000000000000000000000000001010", 18),
STRUCT("dai-dai", "polygon", "DAI", "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063", 18),
STRUCT("usdc-usd-coin", "polygon", "USDC", "0x2791bca1f2de4661ed88a30c99a7a9449aa84174", 6),
STRUCT("usdt-tether", "polygon", "USDT", "0xc2132d05d31c914a87c6611c10748aeb04b58e8f", 6),
STRUCT("stmatic-lido-staked-matic","polygon","stMATIC","0x3a58a54c066fdc0f2d55fc9c89f0415c92ebf3c4",18),
STRUCT("sushi-sushi","polygon","SUSHI","0x0b3f868e0be5597d5db7feb59e1cadbb0fdda50a",18),
STRUCT("wbtc-wrapped-bitcoin", "polygon", "WBTC", "0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6", 8),
STRUCT("bets-betswirl", "polygon", "BETS", "0x9246a5f10a79a5a939b0c2a75a3ad196aafdb43b", 18),
STRUCT("eth-ethereum", "polygon", "WETH", "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619", 18),
STRUCT("wmatic-wrapped-matic-wormhole", "polygon", "WMATIC", "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270", 18),
STRUCT("link-chainlink", "polygon", "LINK", "0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39", 18),
STRUCT("ghst-aavegotchi", "polygon", "GHST", "0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7", 18),
STRUCT("crv-curve-dao-token", "polygon", "CRV", "0x172370d5cd63279efa6d502dab29171933a610af", 18),
STRUCT("mimatic-mimatic", "polygon", "MIMATIC", "0xa3fa99a148fa48d14ed51d610c367c61876997f1", 18),
STRUCT("kom-kommunitas", "polygon", "KOM", "0xc004e2318722ea2b15499d6375905d75ee5390b8", 8),
STRUCT("bob-bob", "polygon", "BOB", "0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b", 18),
STRUCT("ric-ricochet","polygon","RIC","0x263026e7e53dbfdce5ae55ade22493f828922965",18),
STRUCT("nuls-nuls", "polygon", "NULS", "0x8b8e48a8cc52389cd16a162e5d8bd514fabf4ba0", 8),
STRUCT("blank-blockwallet","polygon", "BLANK", "0xf4C83080E80AE530d6f8180572cBbf1Ac9D5d435", 18),
STRUCT("fin-fin", "polygon", "FIN", "0x576c990a8a3e7217122e9973b2230a3be9678e94", 18),
STRUCT("sphere-sphere-finance", "polygon", "SPHERE", "0x62f594339830b90ae4c084ae7d223ffafd9658a7", 18),
STRUCT("luna-luna-wormhole", "polygon", "LUNA", "0x9cd6746665d9557e1b9a775819625711d0693439", 6),
STRUCT("ust-terrausd", "polygon", "USTC", "0x692597b009d13c4049a947cab2239b7d6517875f", 18),
STRUCT("maticx-liquid-staking-matic-pos","polygon","MATICX","0xfa68fb4628dff1028cfec22b4162fccd0d45efb6",18),
STRUCT("zed-zed-run", "polygon", "ZED", "0x5ec03c1f7fa7ff05ec476d19e34a22eddb48acdc", 18),
STRUCT("polydoge-polydoge","polygon","PolyDoge","0x8a953cfe442c5e8855cc6c61b1293fa648bae472",18),
STRUCT("gns-gains-network", "polygon", "GNS", "0xe5417af564e4bfda1c483642db72007871397896", 18),
STRUCT("ico-axelar", "polygon", "AXL", "0x6e4e624106cb12e168e6533f8ec7c82263358940", 6),
STRUCT("frax-frax", "polygon", "FRAX", "0x45c32fa6df82ead1e2ef74d17b76547eddfaff89", 18),
STRUCT("fxs-frax-share", "polygon", "FXS", "0x1a3acf6D19267E2d3e7f898f42803e90C9219062", 18),
STRUCT("sand-the-sandbox", "polygon", "SAND", "0xbbba073c31bf03b8acf7c28ef0738decf3695683", 18),
STRUCT("wsteth-wrapped-liquid-staked-ether-20", "polygon", "WSTETH", "0x03b54a6e9a984069379fae1a4fc4dbae93b3bccd", 18),
STRUCT("lcd-lucidao", "polygon", "LCD", "0xc2a45fe7d40bcac8369371b08419ddafd3131b4a", 18),
STRUCT("revv-revv", "polygon", "REVV", "0x70c006878a5a50ed185ac4c87d837633923de296", 18),
STRUCT("grain-granary","polygon","GRAIN","0x8429d0afade80498eadb9919e41437a14d45a00b",18),
STRUCT("oath-oath","polygon","OATH","0xc2c52ff5134596f5ff1b1204d3304228f2432836",18),
STRUCT("tetu-tetu-reward-token","polygon","TETU","0x255707B70BF90aa112006E1b07B9AeA6De021424",18)])