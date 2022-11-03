{{config(alias='miners',
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')
}}

SELECT DISTINCT array('ethereum') as blockchain,
       miner, 
       'Ethereum Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('ethereum','blocks') }} 
{% if not var('disable_gnosis')  %}
UNION DISTINCT
SELECT DISTINCT array('gnosis') as blockchain,
       miner, 
       'Gnosis Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('gnosis','blocks') }} 
{% endif %}
{% if not var('disable_avalanche_c')  %}
UNION DISTINCT 
SELECT DISTINCT array('avalanche_c') as blockchain,
       miner, 
       'Avalanche Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('avalanche_c','blocks') }} 
{% endif %}
{% if not var('disable_arbitrum')  %}
UNION DISTINCT 
SELECT DISTINCT array('arbitrum') as blockchain,
       miner, 
       'Arbitrum Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('arbitrum','blocks') }} 
{% endif %}
{% if not var('disable_bnb')  %}
UNION DISTINCT
SELECT DISTINCT array('bnb') as blockchain,
       miner, 
       'BNB Chain Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('bnb','blocks') }} 
{% endif %}
{% if not var('disable_optimism')  %}
UNION DISTINCT
SELECT DISTINCT array('optimism') as blockchain,
       miner, 
       'Optimism Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('optimism','blocks') }} 
{% endif %}
