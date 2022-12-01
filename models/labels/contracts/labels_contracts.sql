{{config(alias='contracts',
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')
}}

SELECT array('ethereum') AS blockchain,
       address,
       concat(upper(SUBSTRING(namespace, 1, 1)), SUBSTRING(namespace, 2)) || ': ' || name AS name,
       'contracts' AS category,
       'soispoke' AS contributor,
       'query' AS source,
       date('2022-09-26') AS created_at,
       now() AS modified_at
FROM {{ source('ethereum', 'contracts') }}
UNION
SELECT array('gnosis') AS blockchain,
       address,
       concat(upper(SUBSTRING(namespace, 1, 1)), SUBSTRING(namespace, 2)) || ': ' || name AS name,
       'contracts' AS category,
       'soispoke' AS contributor,
       'query' AS source,
       date('2022-09-26') AS created_at,
       now() AS modified_at
FROM {{ source('gnosis', 'contracts') }}
UNION
SELECT array('avalanche_c') AS blockchain,
       address,
       concat(upper(SUBSTRING(namespace, 1, 1)), SUBSTRING(namespace, 2)) || ': ' || name AS name,
       'contracts' AS category,
       'soispoke' AS contributor,
       'query' AS source,
       date('2022-09-26') AS created_at,
       now() AS modified_at
FROM {{ source('avalanche_c', 'contracts') }}
UNION
SELECT array('arbitrum') AS blockchain,
       address,
       concat(upper(SUBSTRING(namespace, 1, 1)), SUBSTRING(namespace, 2)) || ': ' || name AS name,
       'contracts' AS category,
       'soispoke' AS contributor,
       'query' AS source,
       date('2022-09-26') AS created_at,
       now() AS modified_at
FROM {{ source('arbitrum', 'contracts') }}
UNION
SELECT array('bnb') AS blockchain,
       address,
       concat(upper(SUBSTRING(namespace, 1, 1)), SUBSTRING(namespace, 2)) || ': ' || name AS name,
       'contracts' AS category,
       'soispoke' AS contributor,
       'query' AS source,
       date('2022-09-26') AS created_at,
       now() AS modified_at
FROM {{ source('bnb', 'contracts') }}
UNION
SELECT array('optimism') AS blockchain,
       address,
       concat(upper(SUBSTRING(namespace, 1, 1)), SUBSTRING(namespace, 2)) || ': ' || name AS name,
       'contracts' AS category,
       'soispoke' AS contributor,
       'query' AS source,
       date('2022-09-26') AS created_at,
       now() AS modified_at
FROM {{ source('optimism', 'contracts') }}