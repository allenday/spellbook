{{ config(alias='referrals',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith"]\') }}'
) }}

-- PoC Query: https://dune.com/queries/1789628?d=1
WITH
referral_map AS (
    SELECT DISTINCT
        app_hash,
        referrer
    FROM {{ ref('cow_protocol_ethereum_app_data') }}
    WHERE referrer IS NOT NULL
),

-- Table with first trade per user. Used to determine their referral
ordered_user_trades AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY trader ORDER BY block_time, evt_index) AS user_trade_index,
        trader,
        app_data
    FROM {{ ref('cow_protocol_ethereum_trades') }}
    GROUP BY trader, block_time, app_data, evt_index
),

user_first_trade AS (
    SELECT
        trader,
        app_data
    FROM ordered_user_trades
    -- Only considers app_data from first trade!
    WHERE user_trade_index = 1
),

referrals AS (
    SELECT
        trader,
        referrer
    FROM referral_map
    INNER JOIN user_first_trade
        ON app_hash = app_data
)

SELECT * FROM referrals
