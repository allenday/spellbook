

{{ config(
        schema = 'op_token_optimism',
        alias='initial_allocations'
        , unique_key = ['allocation_category','allocation_subcategory']
        
  )
}}

WITH initial_allocation_percentages AS (
  SELECT pct_supply_allocation, allocation_category, allocation_subcategory
  FROM UNNEST(ARRAY<STRUCT<pct_supply_allocation NUMERIC,allocation_category STRING,allocation_subcategory STRING>> [STRUCT(0.054,'Ecosystem Fund','Governance Fund'),
STRUCT(0.054,'Ecosystem Fund','Partner Fund'),
STRUCT(0.054,'Ecosystem Fund','Seed Fund'),
STRUCT(0.088,'Ecosystem Fund','Unallocated'),
STRUCT(0.20, 'Retroactive Public Goods Funding (RetroPGF)','Retroactive Public Goods Funding (RetroPGF)'),
STRUCT(0.19, 'User Airdrops','User Airdrops'),
STRUCT(0.19, 'Core Contributors', 'Core Contributors'),
STRUCT(0.17, 'Investors', 'Investors')])
)

SELECT

  pct_supply_allocation
, allocation_category
, allocation_subcategory
, pct_supply_allocation*total_initial_supply AS initial_allocated_supply

FROM {{ ref('op_token_optimism_metadata')}} md , initial_allocation_percentages allo