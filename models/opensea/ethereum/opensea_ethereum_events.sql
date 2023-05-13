{{ config(
        alias ='events'
)
}}

SELECT *
FROM
    (
        SELECT
            blockchain,
            project,
            version,
            block_time,
            token_id,
            collection,
            amount_usd,
            token_standard,
            trade_type,
            CAST(number_of_items AS DECIMAL(38, 0)) AS number_of_items,
            trade_category,
            evt_type,
            seller,
            buyer,
            amount_original,
            CAST(amount_raw AS DECIMAL(38, 0)) AS amount_raw,
            currency_symbol,
            currency_contract,
            nft_contract_address,
            project_contract_address,
            aggregator_name,
            aggregator_address,
            tx_hash,
            block_number,
            tx_from,
            tx_to,
            platform_fee_amount_raw,
            platform_fee_amount,
            platform_fee_amount_usd,
            CAST(platform_fee_percentage AS DOUBLE) AS platform_fee_percentage,
            royalty_fee_amount_raw,
            royalty_fee_amount,
            royalty_fee_amount_usd,
            CAST(royalty_fee_percentage AS DOUBLE) AS royalty_fee_percentage,
            royalty_fee_receive_address,
            royalty_fee_currency_symbol,
            unique_trade_id
        FROM {{ ref('opensea_v1_ethereum_events') }}
        UNION ALL
        SELECT
            blockchain,
            project,
            version,
            block_time,
            token_id,
            collection,
            amount_usd,
            token_standard,
            CASE
                WHEN trade_type != 'Bundle Trade' AND count(*) OVER (PARTITION BY tx_hash) > 1 THEN 'Bulk Purchase'
                ELSE trade_type
            END AS trade_type,
            CAST(number_of_items AS DECIMAL(38, 0)) AS number_of_items,
            CASE WHEN is_private THEN 'Private Sale' ELSE trade_category END AS trade_category, -- Private sale can be purchasd by Buy/Offer accepted, but we surpress when it is Private sale here 
            evt_type,
            seller,
            buyer,
            amount_original,
            CAST(amount_raw AS DECIMAL(38, 0)) AS amount_raw,
            currency_symbol,
            currency_contract,
            nft_contract_address,
            project_contract_address,
            aggregator_name,
            aggregator_address,
            tx_hash,
            block_number,
            tx_from,
            tx_to,
            platform_fee_amount_raw,
            platform_fee_amount,
            platform_fee_amount_usd,
            CASE WHEN amount_raw > 0 THEN CAST((platform_fee_amount_raw / amount_raw * 100) AS DOUBLE) END AS platform_fee_percentage,
            royalty_fee_amount_raw,
            royalty_fee_amount,
            royalty_fee_amount_usd,
            CASE WHEN amount_raw > 0 THEN CAST((royalty_fee_amount_raw / amount_raw * 100) AS DOUBLE) END AS royalty_fee_percentage,
            royalty_fee_receive_address,
            currency_symbol AS royalty_fee_currency_symbol,
            unique_trade_id
        FROM {{ ref('opensea_v3_ethereum_events') }}
        UNION ALL
        SELECT
            blockchain,
            project,
            version,
            block_time,
            token_id,
            collection,
            amount_usd,
            token_standard,
            CASE
                WHEN trade_type != 'Bundle Trade' AND count(*) OVER (PARTITION BY tx_hash) > 1 THEN 'Bulk Purchase'
                ELSE trade_type
            END AS trade_type,
            CAST(number_of_items AS DECIMAL(38, 0)) AS number_of_items,
            CASE WHEN is_private THEN 'Private Sale' ELSE trade_category END AS trade_category, -- Private sale can be purchasd by Buy/Offer accepted, but we surpress when it is Private sale here 
            evt_type,
            seller,
            buyer,
            amount_original,
            CAST(amount_raw AS DECIMAL(38, 0)) AS amount_raw,
            currency_symbol,
            currency_contract,
            nft_contract_address,
            project_contract_address,
            aggregator_name,
            aggregator_address,
            tx_hash,
            block_number,
            tx_from,
            tx_to,
            platform_fee_amount_raw,
            platform_fee_amount,
            platform_fee_amount_usd,
            CASE WHEN amount_raw > 0 THEN CAST((platform_fee_amount_raw / amount_raw * 100) AS DOUBLE) END AS platform_fee_percentage,
            royalty_fee_amount_raw,
            royalty_fee_amount,
            royalty_fee_amount_usd,
            CASE WHEN amount_raw > 0 THEN CAST((royalty_fee_amount_raw / amount_raw * 100) AS DOUBLE) END AS royalty_fee_percentage,
            royalty_fee_receive_address,
            currency_symbol AS royalty_fee_currency_symbol,
            unique_trade_id
        FROM {{ ref('opensea_v4_ethereum_events') }}
    )
