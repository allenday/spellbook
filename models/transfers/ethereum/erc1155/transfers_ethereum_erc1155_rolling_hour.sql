{{ config(
        alias ='erc1155_rolling_hour'
        )
}}

        SELECT
            'ethereum' AS blockchain,
            hour,
            wallet_address,
            token_address,
            tokenId,
            cast(current_timestamp AS timestamp) AS updated_at,
            row_number() over (partition BY token_address, tokenId, wallet_address order BY hour desc) AS recency_index,
            sum(amount) over (
                partition BY token_address, wallet_address order BY hour
            ) AS amount
        FROM {{ ref('transfers_ethereum_erc1155_agg_hour') }}