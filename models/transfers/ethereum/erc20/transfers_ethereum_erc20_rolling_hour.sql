{{ config(
        alias ='erc20_rolling_hour'
        )
}}

        SELECT
            'ethereum' AS blockchain,
            hour,
            wallet_address,
            token_address,
            symbol,
            current_timestamp() AS last_updated,
            row_number() over (partition BY token_address, wallet_address order BY hour desc) AS recency_index,
            sum(amount_raw) over (
                partition BY token_address, wallet_address order BY hour
            ) AS amount_raw,
            sum(amount) over (
                partition BY token_address, wallet_address order BY hour
            ) AS amount
        FROM {{ ref('transfers_ethereum_erc20_agg_hour') }}