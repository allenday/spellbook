{{config(alias='ens',
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['blockchain','address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["0xRob"]\') }}')
}}


-- AS default label, take the ENS reverse record or the latest resolver record
SELECT *
FROM (
       SELECT
       array('ethereum') AS blockchain,
       coalesce(rev.address, res.address) AS address,
       coalesce(rev.name, res.name) AS name,
       'ENS' AS category,
       '0xRob' AS contributor,
       'query' AS source,
       date('2022-10-06') AS created_at,
       now() AS modified_at
    FROM (
        SELECT *
        FROM (
            SELECT
                address,
                 name
                 ,row_number() over (partition by address order by block_time asc) AS ordering
            FROM {{ ref('ens_resolver_latest') }}
        ) where ordering = 1
    ) res
    FULL OUTER JOIN {{ ref('ens_reverse_latest') }} rev
    ON res.address = rev.address
) ens

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

