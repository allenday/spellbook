{{ config(
        schema='prices_native',
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
    , CAST(decimals as int) as `decimals`
FROM UNNEST(ARRAY<STRUCT<token_id STRING,blockchain STRING,symbol STRING,contract_address STRING,decimals STRING>> [STRUCT("ada-cardano", null, "ADA", null, null),
STRUCT("ae-aeternity", null, "AE", null, null),
STRUCT("algo-algorand", null, "ALGO", null, null),
STRUCT("atom-cosmos", null, "ATOM", null, null),
STRUCT("avax-avalanche", null, "AVAX", null, null),
STRUCT("bch-bitcoin-cash", null, "BCH", null, null),
STRUCT("bnb-binance-coin", null, "BNB", null, null),
STRUCT("bsv-bitcoin-sv", null, "BSV", null, null),
STRUCT("btc-bitcoin", null, "BTC", null, null),
STRUCT("dash-dash", null, "DASH", null, null),
STRUCT("dcr-decred", null, "DCR", null, null),
STRUCT("doge-dogecoin", null, "DOGE", null, null),
STRUCT("eos-eos", null, "EOS", null, null),
STRUCT("etc-ethereum-classic", null, "ETC", null, null),
STRUCT("eth-ethereum", null, "ETH", null, null),
STRUCT("ftm-fantom", null, "FTM", null, null),
STRUCT("hbar-hedera-hashgraph", null, "HBAR", null, null),
STRUCT("icx-icon", null, "ICX", null, null),
STRUCT("ltc-litecoin", null, "LTC", null, null),
STRUCT("matic-polygon", null, "MATIC", null, null),
STRUCT("miota-iota", null, "MIOTA", null, null),
STRUCT("mona-monacoin", null, "MONA", null, null),
STRUCT("neo-neo", null, "NEO", null, null),
STRUCT("ont-ontology", null, "ONT", null, null),
STRUCT("sol-solana", null, "SOL", null, null),
STRUCT("stx-blockstack", null, "STX", null, null),
STRUCT("thr-thorecoin", null, "THR", null, null),
STRUCT("tomo-tomochain", null, "TOMO", null, null),
STRUCT("trx-tron", null, "TRX", null, null),
STRUCT("xdai-xdai", null, "XDAI", null, null),
STRUCT("xem-nem", null, "XEM", null, null),
STRUCT("xlm-stellar", null, "XLM", null, null),
STRUCT("xmr-monero", null, "XMR", null, null),
STRUCT("xrp-xrp", null, "XRP", null, null),
STRUCT("xtz-tezos", null, "XTZ", null, null),
STRUCT("zec-zcash", null, "ZEC", null, null)])