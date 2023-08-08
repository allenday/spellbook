{{ config(
    schema ='pooltogether_v4_ethereum',
    alias ='prize_structure',
    materialized = 'view',
            unique_key = ['tx_hash', 'network']
)}}

WITH
  --Calculate prize structure for Ethereum network per drawID
prize_distribution AS (
    --ETHEREUM POST DPR
    SELECT call_tx_hash                                                        AS tx_hash,
           call_block_time                                                     AS block_time,
           'Ethereum'                                                          AS network,
           drawId,
           JSON_EXTRACT_SCALAR(output_0, '$.bitRangeSize')                         AS bitRange,
           substr(
                   JSON_EXTRACT_SCALAR(output_0, '$.tiers'),
                   2,
                   length(JSON_EXTRACT_SCALAR(output_0, '$.tiers')) - 2
               )                                                               AS tiers,
           CAST(JSON_EXTRACT_SCALAR(output_0, '$.dpr') AS int) / power(10, 6)      AS dpr,
           CAST(JSON_EXTRACT_SCALAR(output_0, '$.prize') AS FLOAT64) / power(10, 6) AS prize
    FROM
      {{ source('pooltogether_v4_ethereum', 'PrizeTierHistoryV2_call_getPrizeTier')}}
    WHERE call_success = true
    {% if is_incremental() %}
      AND call_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}

    UNION ALL

    --ETHEREUM PRE DPR
    SELECT evt_tx_hash                                                                  AS tx_hash,
           evt_block_time,
           'Ethereum'                                                                   AS network,
           drawId,
           JSON_EXTRACT_SCALAR(prizeDistribution, '$.bitRangeSize')                         AS bitRange,
           substr(
                   JSON_EXTRACT_SCALAR(prizeDistribution, '$.tiers'),
                   2,
                   length(JSON_EXTRACT_SCALAR(prizeDistribution, '$.tiers')) - 2
               )                                                                        AS tiers,
           0                                                                            AS dpr,
           CAST(JSON_EXTRACT_SCALAR(prizeDistribution, '$.prize') AS FLOAT64) / power(10, 6) AS prize
    FROM
      {{ source('pooltogether_v4_ethereum', 'PrizeDistributionBuffer_evt_PrizeDistributionSet')}}
    WHERE drawID < 447 --DPR On Ethereum started on draw 447
    {% if is_incremental() %}
      AND evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
),

detailed_prize_distribution AS (
    SELECT tx_hash,
           block_time,
           network,
           drawId                                  AS draw_id,
           CAST(bitRange AS int)                   AS bit_range,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(1)\] AS int)  AS tiers1,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(2)\] AS int)  AS tiers2,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(3)\] AS int)  AS tiers3,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(4)\] AS int)  AS tiers4,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(5)\] AS int)  AS tiers5,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(6)\] AS int)  AS tiers6,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(7)\] AS int)  AS tiers7,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(8)\] AS int)  AS tiers8,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(9)\] AS int)  AS tiers9,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(10)\] AS int) AS tiers10,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(11)\] AS int) AS tiers11,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(12)\] AS int) AS tiers12,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(13)\] AS int) AS tiers13,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(14)\] AS int) AS tiers14,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(15)\] AS int) AS tiers15,
           CAST(SPLIT\(tiers, ','\)\[SAFE_OFFSET(16)\] AS int) AS tiers16,
           dpr,
           prize
    FROM prize_distribution
)

SELECT tx_hash,
       block_time,
       network,
       draw_id,
       bit_range,
       tiers1,
       tiers2,
       tiers3,
       tiers4,
       tiers5,
       tiers6,
       tiers7,
       tiers8,
       tiers9,
       tiers10,
       tiers11,
       tiers12,
       tiers13,
       tiers14,
       tiers15,
       tiers16,
       dpr,
       prize
FROM detailed_prize_distribution