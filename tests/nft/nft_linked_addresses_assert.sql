-- Check against manually selected seed data
WITH match_address_count AS (
    SELECT COUNT(*) AS address_count
    FROM {{ ref('nft_linked_addresses') }} AS a
    INNER JOIN
        {{ ref('nft_linked_addresses_postgres') }} AS test_data ON
            test_data.master_address = a.master_address AND test_data.alternative_address = a.alternative_address
),

seed_data_count AS (
    SELECT COUNT(*) AS seed_address_count
    FROM {{ ref('nft_linked_addresses_postgres') }}
)

SELECT 1 AS result
FROM match_address_count
INNER JOIN seed_data_count
WHERE address_count < seed_address_count * 0.95
