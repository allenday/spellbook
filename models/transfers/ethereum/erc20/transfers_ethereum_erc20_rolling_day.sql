{{ config(
        alias ='erc20_rolling_day')
}}

        SELECT
            'ethereum' AS blockchain,
            day,
            wallet_address,
            token_address,
            symbol,
            current_timestamp() AS last_updated,
            row_number() over (partition by token_address, wallet_address order by day desc) AS recency_index,
            sum(amount_raw) over (
                partition by token_address, wallet_address order by day
            ) AS amount_raw,
            sum(amount) over (
                partition by token_address, wallet_address order by day
            ) AS amount
        FROM {{ ref('transfers_ethereum_erc20_agg_day') }}