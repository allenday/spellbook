{{config(alias='contracts',
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')
}}

SELECT array('ethereum') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       CURRENT_TIMESTAMP as modified_at
FROM {{ source('ethereum','contracts') }} 
UNION DISTINCT
SELECT array('gnosis') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       CURRENT_TIMESTAMP as modified_at
FROM {{ source('gnosis','contracts') }} 
UNION DISTINCT
SELECT array('avalanche_c') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       CURRENT_TIMESTAMP as modified_at
FROM {{ source('avalanche_c','contracts') }} 
UNION DISTINCT
SELECT array('arbitrum') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       CURRENT_TIMESTAMP as modified_at
FROM {{ source('arbitrum','contracts') }} 
UNION DISTINCT
SELECT array('bnb') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       CURRENT_TIMESTAMP as modified_at
FROM {{ source('bnb','contracts') }} 
UNION DISTINCT
SELECT array('optimism') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       CURRENT_TIMESTAMP as modified_at
FROM {{ source('optimism','contracts') }} 
