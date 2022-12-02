{{ config(
        alias ='erc1155_rolling_day'
        )
}}

        SELECT
            'ethereum' AS blockchain
            , day
            , wallet_address
            , token_address
            , tokenId
            , current_timestamp() AS updated_at
            , row_number() OVER (
                PARTITION BY
                    token_address, tokenId, wallet_address
                ORDER BY day DESC
            ) AS recency_index
            , sum(amount) OVER (
                PARTITION BY token_address, wallet_address ORDER BY day
            ) AS amount
        FROM {{ ref('transfers_ethereum_erc1155_agg_day') }}
