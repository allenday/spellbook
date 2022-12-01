-- trades between block 15432470 and 15432500
WITH trades AS (
    SELECT
        block_number,
        tx_hash,
        trade_category,
        nft_contract_address,
        token_id,
        amount_original,
        platform_fee_amount,
        pool_fee_amount
    FROM {{ ref('sudoswap_ethereum_events') }}
    WHERE block_number >= 15432470 AND block_number < 15432500
),

examples AS (
    SELECT * FROM {{ ref('sudoswap_ethereum_example_trades') }}
),

matched AS (
    SELECT
        trades.amount_original AS reported_amount_original,
        examples.amount_original AS seed_amount_original,
        trades.platform_fee_amount AS reported_platform_fee_amount,
        examples.platform_fee_amount AS seed_platform_fee_amount,
        trades.pool_fee_amount AS reported_pool_fee_amount,
        examples.pool_fee_amount AS seed_pool_fee_amount,
        coalesce(trades.block_number, examples.block_number),
        coalesce(trades.tx_hash, examples.tx_hash),
        coalesce(trades.nft_contract_address, examples.nft_contract_address),
        coalesce(trades.token_id, examples.token_id),
        coalesce((trades.amount_original = examples.amount_original),
            FALSE) AS correct_amt_orig,
        coalesce((trades.platform_fee_amount = examples.platform_fee_amount),
            FALSE) AS correct_platform_fee,
        coalesce((trades.pool_fee_amount = examples.pool_fee_amount),
            FALSE) AS correct_pool_fee
    FROM trades
    FULL OUTER JOIN examples
        ON
            examples.block_number = trades.block_number AND examples.tx_hash = trades.tx_hash
            AND examples.nft_contract_address = trades.nft_contract_address AND examples.token_id = trades.token_id
)

SELECT * FROM matched
WHERE not(correct_amt_orig AND correct_platform_fee AND correct_pool_fee)
