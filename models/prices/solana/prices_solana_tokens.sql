{{ config(
        schema='prices_solana',
        alias ='tokens',
        materialized = 'view',
                tags=['static']
        )
}}
SELECT 
    TRIM(token_id) as token_id
    , LOWER(TRIM(blockchain)) as blockchain
    , TRIM(symbol) as symbol
    , TRIM(contract_address) as contract_address 
    , decimals
FROM UNNEST(ARRAY<STRUCT<token_id STRING,blockchain STRING,symbol STRING,contract_address STRING,decimals INT64>> [STRUCT("ftt-ftx-token","solana","FTT","AGFEad2et2ZJif9jaGpdMixQqvW5i81aBdvKe7PHNfz3",6),
STRUCT("forge-blocksmith-labs-forge","solana","FORGE","FoRGERiW7odcCBGU1bztZi16osPBHjxharvDathL5eds",9),
STRUCT("gmt-stepn","solana","STEPN-GMT","7i5KKsX2weiTkry7jA4ZwSuXGhs5eJBEjY8vVxR4pfRx",9),
STRUCT("bonk-bonk","solana","BONK","DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",5),
STRUCT("rlb-rollbit-coin","solana","RLB","RLBxxFkseAZ4RgJH3Sqn8jXxhmGoz9jWxDNJMh8pL7a",6),
STRUCT("orca-orca","solana","ORCA","orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE",6),
STRUCT("uxd-uxd-stablecoin","solana","UXD","7kbnvuGBxxj8AG9qp8Scn56muWGaRaFqxg1FsRp3PaFT",6),
STRUCT("aury-aurory", "solana", "AURY", "AURYydfxJib1ZkTir1Jn1J9ECYUtjb6rKQVmtYaixWPP", 9),
STRUCT("btc-bitcoin", "solana", "BTC", "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E", 6),
STRUCT("cope-cope", "solana", "COPE", "8HGyAAB1yoM1ttS7pXjHMa3dukTFGQggnFFH3hJZgzQh", 6),
STRUCT("dfl-defi-land", "solana", "DFL", "DFL1zNkaGPWm1BqAVqRjCZvHmwTFrEaJtbzJWgseoNJh", 9),
STRUCT("dust-dust-protocol", "solana", "DUST", "DUSTawucrTsGU8hcqRdHDCbuYhCPADMLM2VcCb8VnFnQ", 9),
STRUCT("eth-ethereum", "solana", "ETH", "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs", 8),
STRUCT("eth-ethereum", "solana", "soETH", "2FPyTwcZLUg1MDrwsyoP4D6s1tM7hAkHYRjkNb5w6Pxk", 6),
STRUCT("gst-gst", "solana", "GST", "AFbX8oGjGpmVFywbVouvhQSRmiW2aR1mohfahi4Y2AdB", 9),
STRUCT("luna-terra", "solana", "LUNA", "F6v4wfAdJB8D8p77bMXZgYt8TDKsYxLYxH5AFhUkYx9W", 6),
STRUCT("mngo-mango-markets", "solana", "MNGO", "MangoCzJ36AjZyKwVj3VnYU4GTonjfVEnJmvvWaxLac", 6),
STRUCT("msol-marinade-staked-sol", "solana", "MSOL", "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So", 9),
STRUCT("ray-raydium", "solana", "RAY", "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R", 6),
STRUCT("samo-samoyedcoin", "solana", "SAMO", "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU", 9),
STRUCT("shdw-genesysgo-shadow", "solana", "SHDW", "SHDWyBxihqiCj6YekG2GUr7wqKLeLAMK1gHZck9pL6y", 9),
STRUCT("srm-serum", "solana", "SRM", "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt", 6),
STRUCT("steth-lido-staked-ether", "solana", "wstETH", "ZScHuTtqZukUrtZS43teTKGs2VqkKL8k4QCouR2n6Uo", 8),
STRUCT("stsol-lido-staked-sol", "solana", "stSOL", "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj", 9),
STRUCT("sol-solana", "solana", "SOL", "So11111111111111111111111111111111111111112", 9),
STRUCT("ust-terrausd", "solana", "USTC", "9vMJfxuKxXBoEa7rM12mYLMwTacLMLDJqHozw96WQL8i", 6),
STRUCT("usdc-usd-coin", "solana", "USDC", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", 6),
STRUCT("usdt-tether", "solana", "USDT", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", 6)])