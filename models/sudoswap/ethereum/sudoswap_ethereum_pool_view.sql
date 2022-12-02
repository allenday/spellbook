{{
config(
      alias='pools',
      post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "sudoswap",
                                  \'["niftytable", "0xRob"]\') }}')
}}

WITH
pool_balance AS (
    SELECT
        pool_address
        , SUM(eth_balance_change) AS eth_balance
        , SUM(nft_balance_change) AS nft_balance
    FROM
        {{ ref('sudoswap_ethereum_pool_balance_agg_day') }}
    GROUP BY pool_address
)

, pool_trade_stats AS (
    SELECT
        pool_address
        , SUM(eth_volume) AS eth_volume
        , SUM(nfts_traded) AS nfts_traded
        , SUM(usd_volume) AS usd_volume
        , SUM(pool_fee_volume_eth) AS pool_fee_volume_eth
        , SUM(pool_fee_bid_volume_eth) AS pool_fee_bid_volume_eth
        , SUM(pool_fee_ask_volume_eth) AS pool_fee_ask_volume_eth
        , SUM(platform_fee_volume_eth) AS platform_fee_volume_eth
        , SUM(eth_change_trading) AS eth_change_trading
        , SUM(nft_change_trading) AS nft_change_trading
    FROM
        {{ ref('sudoswap_ethereum_pool_trades_agg_day') }}
    GROUP BY pool_address
)

SELECT
    p.pool_address
    , p.nft_contract_address
    , creator_address
    , eth_balance
    , nft_balance
    , pool_type
    , p.bonding_curve
    , s.spot_price
    , s.delta
    , s.pool_fee
    , p.spot_price AS initial_spot_price
    , initial_nft_balance
    , initial_eth_balance
    , pool_factory
    , creation_block_time
    , creation_tx_hash
    , COALESCE(eth_volume, 0) AS eth_volume
    , COALESCE(usd_volume, 0) AS usd_volume
    , COALESCE(nfts_traded, 0) AS nfts_traded
    , COALESCE(pool_fee_volume_eth, 0) AS pool_fee_volume_eth
    , COALESCE(pool_fee_bid_volume_eth, 0) AS pool_fee_bid_volume_eth
    , COALESCE(pool_fee_ask_volume_eth, 0) AS pool_fee_ask_volume_eth
    , COALESCE(platform_fee_volume_eth, 0) AS platform_fee_volume_eth
    , COALESCE(eth_change_trading, 0) AS eth_change_trading
    , COALESCE(nft_change_trading, 0) AS nft_change_trading
FROM {{ ref('sudoswap_ethereum_pool_creations') }} AS p
INNER JOIN {{ ref('sudoswap_ethereum_pool_settings_latest') }} AS s
    ON p.pool_address = s.pool_address
INNER JOIN pool_balance ON p.pool_address = pool_balance.pool_address
LEFT JOIN pool_trade_stats ON p.pool_address = pool_trade_stats.pool_address
