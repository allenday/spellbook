{{ config(
        schema = 'op_chains',
        alias='chain_list'
        , unique_key = ['blockchain', 'chain_id']
        
  )
}}

SELECT 
        lower(blockchain) AS blockchain,
        blockchain_name,
        chain_id,
        cast(start_date AS date) AS start_date,
        is_superchain

FROM UNNEST(ARRAY<STRUCT<blockchain STRING,blockchain_name STRING,chain_id INT64,start_date STRING,is_superchain INT64>> [STRUCT('optimism',   'Optimism Mainnet', 10, '2021-06-23',   1),
STRUCT('base',       'Base Mainnet',     NULL,   NULL,       1)])