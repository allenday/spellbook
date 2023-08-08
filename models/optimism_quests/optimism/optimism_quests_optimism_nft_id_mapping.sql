{{config(
        schema='optimism_quests_optimism',
        alias='nft_id_mapping')}}



with quest_nft_ids AS (
    SELECT contract_project, quest_project, cast(nft_id as STRING) as nft_id
    FROM UNNEST(ARRAY<STRUCT<contract_project STRING,quest_project STRING,nft_id INT64>> [STRUCT('Beethoven X', 'Beethoven X', 6366),
STRUCT('Clipper','Clipper', 6357),
STRUCT('Hop Protocol', 'Hop', 6359),
STRUCT('Kwenta','Kwenta', 6364),
STRUCT('Lyra','Lyra', 6358),
STRUCT('Perpetual Protocol','Perpetual Protocol', 6349),
STRUCT('Pika Protocol','Pika', 6361),
STRUCT('Polynomial Protocol','Polynomial', 6346),
STRUCT('PoolTogether','PoolTogether', 6351),
STRUCT('QiDao','QiDao', 6363),
STRUCT('Quix','Quix', 6369),
STRUCT('Rubicon','Rubicon', 6360),
STRUCT('Stargate Finance','Stargate', 6340),
STRUCT('Synapse','Synapse', 6347),
STRUCT('Synthetix','Synthetix', 6362),
STRUCT('Granary','The Granary', 6367),
STRUCT('Uniswap','Uniswap', 6343),
STRUCT('Velodrome','Velodrome', 6344)])
)

SELECT * FROM quest_nft_ids