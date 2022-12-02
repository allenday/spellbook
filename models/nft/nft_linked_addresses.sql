{{ config(
    alias = 'linked_addresses',
    partition_by = ['blockchain'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'linked_address_id'],
    post_hook='{{ expose_spells(\'["ethereum", "solana"]\',
                                "sector",
                                "nft",
                                \'["springzh"]\') }}'
    )
}}

WITH nft_trade_address AS (
    SELECT DISTINCT
        blockchain
        , buyer AS address_a
        , seller AS address_b
    FROM {{ ref('nft_trades') }}
    WHERE buyer IS NOT NULL
        AND seller IS NOT NULL
        AND blockchain IS NOT NULL
        {% if is_incremental() %}
            AND block_time >= date_trunc("day", now() - INTERVAL '1 week')
        {% endif %}
        UNION ALL

        SELECT DISTINCT
            blockchain
            , seller AS address_a
            , buyer AS address_b
        FROM {{ ref('nft_trades') }}
        WHERE buyer IS NOT NULL
            AND seller IS NOT NULL
            AND blockchain IS NOT NULL
            {% if is_incremental() %}
                AND block_time >= date_trunc("day", now() - INTERVAL '1 week')
            {% endif %}
)

, linked_address_nft_trade AS (
    SELECT
        blockchain
        , address_a
        , address_b
        , count(*) AS cnt
    FROM nft_trade_address
    GROUP BY 1, 2, 3
    HAVING count(*) > 1
)

, linked_address_sorted AS (
    -- Normalize linked addresses to master address
    SELECT
        blockchain
        , (CASE WHEN address_a > address_b THEN address_b ELSE address_a END) AS master_address
        , address_a AS alternative_address
    FROM linked_address_nft_trade
    UNION
    SELECT
        blockchain
        , (CASE WHEN address_a > address_b THEN address_b ELSE address_a END) AS master_address
        , address_b AS alternative_address
    FROM linked_address_nft_trade
)

, linked_address_sorted_row_num AS (
    SELECT
        blockchain
        , master_address
        , alternative_address
        , master_address || "-" || alternative_address AS linked_address_id
        , ROW_NUMBER() OVER (PARTITION BY blockchain, alternative_address ORDER BY master_address) AS row_num
    FROM linked_address_sorted
)

SELECT
    blockchain
    , master_address
    , alternative_address
    , linked_address_id
FROM linked_address_sorted_row_num
WHERE row_num = 1
