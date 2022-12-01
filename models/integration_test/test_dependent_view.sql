{{ config(
        alias ='test_dependent_view'
        )
}}

        SELECT
            'ethereum' AS blockchain,
            day,
            wallet_address,
            token_address,
            tokenId,
            current_timestamp() AS updated_at,
            row_number() over (partition BY token_address, tokenId, wallet_address order BY day desc) AS recency_index,
            sum(amount) over (
                partition BY token_address, wallet_address order BY day
            ) AS amount
        FROM {{ ref('test_incremental_table') }}