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
            row_number() over (partition by token_address, tokenId, wallet_address order by day desc) AS recency_index,
            sum(amount) over (
                partition by token_address, wallet_address order by day
            ) AS amount
        from {{ ref('test_incremental_table') }}