{{ config(
        alias = 'verse'
        )
}}


select contract_address, project_id, project_id_base_value, collection_name, artist_name, art_collection_unique_id
FROM UNNEST(ARRAY<STRUCT<contract_address STRING,project_id INT64,project_id_base_value INT64,collection_name STRING,artist_name STRING,art_collection_unique_id STRING>> [STRUCT('0xbb5471c292065d3b01b2e81e299267221ae9a250', 0, 1000000, 'Hypertype', 'Mark Webster', '0xbb5471c292065d3b01b2e81e299267221ae9a250-0'),
STRUCT('0xbb5471c292065d3b01b2e81e299267221ae9a250', 1, 1000000, 'Co(r)ral', 'Phaust', '0xbb5471c292065d3b01b2e81e299267221ae9a250-1')])
    
order by project_id asc