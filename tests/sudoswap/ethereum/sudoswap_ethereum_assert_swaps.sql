-- -- test to see if # trades in raw = # trades in processed.
WITH
raw_swaps AS (
    WITH
    pairs_created AS (
        SELECT
            _nft AS nftcontractaddress,
            _initialnftids AS nft_ids,
            _fee AS initialfee,
            _assetrecipient AS asset_recip,
            output_pair AS pair_address,
            call_block_time AS block_time
        FROM
            {{ source('sudo_amm_ethereum','LSSVMPairFactory_call_createPairETH') }}
        WHERE
            call_success
            AND _nft = '0xef1a89cbfabe59397ffda11fc5df293e9bc5db90'
    ),

    fil_swaps AS (
        SELECT *
        FROM
            (
                SELECT
                    contract_address,
                    call_tx_hash,
                    call_trace_address,
                    call_block_time,
                    call_block_number,
                    call_success,
                    tokenrecipient AS call_from,
                    'Sell' AS trade_category
                FROM
                    {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapNFTsForToken') }}
                INNER JOIN
                    pairs_created ON
                        contract_address = pairs_created.pair_address
                WHERE
                    call_block_time >= '2022-07-10'
                    AND call_block_time <= '2022-08-10'
                    AND call_success = true
                UNION ALL
                SELECT
                    contract_address,
                    call_tx_hash,
                    call_trace_address,
                    call_block_time,
                    call_block_number,
                    call_success,
                    nftrecipient AS call_from,
                    'Buy' AS trade_category
                FROM
                    {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapTokenForAnyNFTs') }}
                INNER JOIN
                    pairs_created ON
                        contract_address = pairs_created.pair_address
                WHERE
                    call_block_time >= '2022-07-10'
                    AND call_block_time <= '2022-08-10'
                    AND call_success = true
                UNION ALL
                SELECT
                    contract_address,
                    call_tx_hash,
                    call_trace_address,
                    call_block_time,
                    call_block_number,
                    call_success,
                    nftrecipient AS call_from,
                    'Buy' AS trade_category
                FROM
                    {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapTokenForSpecificNFTs') }}
                INNER JOIN
                    pairs_created ON
                        contract_address = pairs_created.pair_address
                WHERE
                    call_block_time >= '2022-07-10'
                    AND call_block_time <= '2022-08-10'
                    AND call_success = true
            ) AS s
    )

    SELECT COUNT(DISTINCT call_tx_hash) AS num_txs
    FROM
        fil_swaps
),

abstractions_swaps AS (
    SELECT COUNT(DISTINCT tx_hash) AS num_txs
    FROM
        {{ ref('nft_trades') }}
    WHERE
        blockchain = 'ethereum'
        AND project = 'sudoswap'
        AND version = 'v1'
        AND block_time >= '2022-07-10'
        AND block_time <= '2022-08-10'
        AND nft_contract_address = '0xef1a89cbfabe59397ffda11fc5df293e9bc5db90'
),

test AS (
    SELECT (
        SELECT num_txs
        FROM
            abstractions_swaps
    ) - (
        SELECT num_txs
        FROM
            raw_swaps
    ) AS txs_mismatch
)

SELECT *
FROM
    test
WHERE
    txs_mismatch > 0
