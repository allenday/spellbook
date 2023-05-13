{{ config(alias='three_letter_ens_count') }}

WITH three_letter_ens_count AS (
    SELECT
        owner,
        count(owner) AS ens_count
    FROM
        {{ ref('ens_view_registrations') }}
    WHERE
        length(name) = 3

    GROUP BY owner
    ORDER BY ens_count DESC
)

SELECT
    'ethereum' AS blockchain,
    (CONCAT('0x', substring(cast(owner AS string), 3))) AS address,
    'most_three_letter_ens_owner' AS model_name,
    'spanish-or-vanish' AS contributor,
    'query' AS source,
    'social' AS category,
    timestamp('2022-03-03') AS created_at,
    now() AS updated_at,
    'personas' AS label_type,
    concat('Number of three letter ENS Domains owned: ', ens_count) AS name
FROM three_letter_ens_count
WHERE owner IS NOT NULL
