{{ config(
        alias='erc1155_latest',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke"]\') }}'
        )
}}
SELECT
    wallet_address
    , token_address
    , tokenId
    , amount
    , nft_tokens.name AS collection
    , nft_tokens.category AS category
    , updated_at
FROM {{ ref('transfers_ethereum_erc1155_rolling_hour') }}
LEFT JOIN
    {{ ref('tokens_nft') }} AS nft_tokens ON
        nft_tokens.contract_address = token_address
        AND nft_tokens.blockchain = 'ethereum'
