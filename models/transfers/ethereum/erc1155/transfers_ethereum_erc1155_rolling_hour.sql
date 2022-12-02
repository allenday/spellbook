{{ config(
        alias ='erc1155_rolling_hour'
        )
}}

        SELECT
            'ethereum' AS blockchain
            , hour
            , wallet_address
            , token_address
            , tokenId
            , cast(current_timestamp AS timestamp) AS updated_at
            , ROW_NUMBER() OVER (PARTITION BY token_address, tokenId, wallet_address ORDER BY hour DESC) AS recency_index
            , sum(amount) OVER (
                PARTITION BY token_address, wallet_address ORDER BY hour
            ) AS amount
        FROM {{ ref('transfers_ethereum_erc1155_agg_hour') }}
