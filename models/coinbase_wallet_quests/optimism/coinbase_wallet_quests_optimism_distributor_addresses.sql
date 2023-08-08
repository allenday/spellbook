{{config(
        schema='coinbase_wallet_quests_optimism',
        alias='distributor_addresses')}}


{% set op_token = '0x4200000000000000000000000000000000000042' %}

WITH quest_addresses AS (
SELECT lower(distributor_address) AS distributor_address, rewards_token, quest_name
FROM UNNEST(ARRAY<STRUCT<distributor_address STRING,rewards_token STRING,quest_name STRING>> [STRUCT('0xf42279467D821bCDf40b50E9A5d2cACCc4Cf5b30','{{op_token}}','Quest 1 - DEX'),
STRUCT('0x9F4F2B8BdA8D2d3832021b3119747470ea86A183','{{op_token}}','Quest 2 - Delegation'),
STRUCT('0x1fe95e0497a0E38AFBE18Bd19B9a2b42116880f0','{{op_token}}','Quest 3 - Attestation'),
STRUCT('0x12d9aEF514EE8Bc3f7B2d523ae26164632b71acB','{{op_token}}','Quest 4 - Deposit')])

)

SELECT distinct 
    distributor_address, rewards_token, quest_name
    FROM quest_addresses