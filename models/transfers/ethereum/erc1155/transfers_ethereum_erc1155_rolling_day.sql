{{ config(
        alias ='erc1155_rolling_day'
        )
}}

        SELECT
            'ethereum' AS blockchain,
            day,
            wallet_address,
            token_address,
            tokenId,
            current_timestamp() AS updated_at,
            row_number() over (partition by token_address, tokenId, wallet_address order by day desc) AS recency_index,
            sum(amount) over (
                partition by token_address, wallet_address order by day
            ) AS amount
        FROM {{ ref('transfers_ethereum_erc1155_agg_day') }}