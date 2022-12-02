{{ config(
        alias='erc1155_hour',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke"]\') }}'
        )
}}

WITH
hours AS (
    SELECT explode(
        sequence(
            to_date('2015-01-01'), date_trunc('hour', now()), INTERVAL 1 HOUR
        )
    ) AS hour
)

, hourly_balances AS (SELECT
    wallet_address
    , token_address
    , tokenId
    , hour
    , amount
    , lead(
        hour, 1, now()
    ) OVER (
        PARTITION BY token_address, wallet_address ORDER BY hour
    ) AS next_hour
    FROM {{ ref('transfers_ethereum_erc1155_rolling_hour') }}
)

SELECT
    'ethereum' AS blockchain
    , hours.hour
    , hourly_balances.wallet_address
    , hourly_balances.token_address
    , hourly_balances.tokenId
    , hourly_balances.amount
    , nft_tokens.name AS collection
    , nft_tokens.category AS category
FROM hourly_balances
INNER JOIN
    hours ON
        hourly_balances.hour <= hours.hour AND hours.hour < hourly_balances.next_hour
LEFT JOIN
    {{ ref('tokens_nft') }} AS nft_tokens ON
        nft_tokens.contract_address = hourly_balances.token_address
        AND nft_tokens.blockchain = 'ethereum'
