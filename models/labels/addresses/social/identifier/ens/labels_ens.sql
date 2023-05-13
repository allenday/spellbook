{{ config(alias='ens',
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['blockchain','address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["0xRob"]\') }}')
}}


-- as default label, take the ENS reverse record or the latest resolver record
SELECT *
FROM (
    SELECT
        'ethereum' AS blockchain,
        coalesce(rev.address, res.address) AS address,
        coalesce(rev.name, res.name) AS name,
        'ENS' AS category, --should be social but we can't change this due to how many queries it probably breaks.
        '0xRob' AS contributor,
        'query' AS source,
        date('2022-10-06') AS created_at,
        now() AS updated_at,
        'ens' AS model_name,
        'identifier' AS label_type
    FROM (
        SELECT *
        FROM (
            SELECT
                address,
                name,
                row_number() OVER (PARTITION BY address ORDER BY block_time ASC) AS ordering
            FROM {{ ref('ens_resolver_latest') }}
        ) WHERE ordering = 1
    ) AS res
    FULL OUTER JOIN {{ ref('ens_reverse_latest') }} AS rev
        ON res.address = rev.address
) AS ens

-- For now, we want to limit the amount of ENS labels to 1
--UNION
--SELECT 'ethereum' as blockchain,
--       address,
--       name,
--       'ENS resolver' as category,
--       '0xRob' as contributor,
--       'query' AS source,
--       date('2022-10-06') as created_at,
--       now() as modified_at
--FROM {{ ref('ens_resolver_latest') }}
--UNION
--SELECT 'ethereum' as blockchain,
--       address,
--       name,
--       'ENS reverse' as category,
--       '0xRob' as contributor,
--       'query' AS source,
--       date('2022-10-06') as created_at,
--       now() as modified_at
--FROM {{ ref('ens_reverse_latest') }}
