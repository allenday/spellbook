{{config(alias='contracts')
}}

SELECT 'ethereum' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       CURRENT_TIMESTAMP() as updated_at,
        'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('ethereum','contracts') }} 
UNION ALL 
SELECT 'gnosis' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       CURRENT_TIMESTAMP() as updated_at,
        'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('gnosis','contracts') }} 
UNION ALL 
SELECT 'avalanche_c' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       CURRENT_TIMESTAMP() as updated_at,
        'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('avalanche_c','contracts') }} 
UNION ALL 
SELECT 'arbitrum' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       CURRENT_TIMESTAMP() as updated_at,
       'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('arbitrum','contracts') }} 
UNION ALL 
SELECT 'bnb' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       CURRENT_TIMESTAMP() as updated_at,
       'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('bnb','contracts') }} 
UNION ALL 
SELECT 'optimism' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       CURRENT_TIMESTAMP() as updated_at,
       'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('optimism','contracts') }} 
UNION ALL 
SELECT 'fantom' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'Henrystats' as contributor,
       'query' AS source,
       date('2022-12-18') as created_at,
       CURRENT_TIMESTAMP() as updated_at,
       'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('fantom','contracts') }} 
UNION ALL 
SELECT 'polygon' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'Henrystats' as contributor,
       'query' AS source,
       date('2023-01-27') as created_at,
       CURRENT_TIMESTAMP() as updated_at,
       'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('polygon','contracts') }}