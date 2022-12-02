{{ config(alias='ens',
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['blockchain', 'address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["0xRob"]\') }}')
}}


-- AS default label, take the ENS reverse record or the latest resolver record
SELECT *
FROM (
    SELECT
        'ENS' AS category
        , '0xRob' AS contributor
        , 'query' AS source
        , array('ethereum') AS blockchain
        , coalesce(rev.address, res.address) AS address
        , coalesce(rev.name, res.name) AS name
        , date('2022-10-06') AS created_at
        , now() AS modified_at
    FROM (
        SELECT *
        FROM (
            SELECT
                address
                , name
                , row_number() OVER (
                    PARTITION BY address ORDER BY block_time ASC
                ) AS ordering
            FROM {{ ref('ens_resolver_latest') }}
        ) WHERE ordering = 1
    ) AS res
    FULL OUTER JOIN {{ ref('ens_reverse_latest') }} AS rev
        ON res.address = rev.address
) AS ens

-- For now, we want to limit the amount of ENS labels to 1
--UNION
--SELECT array('ethereum') AS blockchain,
--       address,
--       name,
--       'ENS resolver' AS category,
--       '0xRob' AS contributor,
--       'query' AS source,
--       date('2022-10-06') AS created_at,
--       now() AS modified_at
--FROM {{ ref('ens_resolver_latest') }}
--UNION
--SELECT array('ethereum') AS blockchain,
--       address,
--       name,
--       'ENS reverse' AS category,
--       '0xRob' AS contributor,
--       'query' AS source,
--       date('2022-10-06') AS created_at,
--       now() AS modified_at
--FROM {{ ref('ens_reverse_latest') }}
