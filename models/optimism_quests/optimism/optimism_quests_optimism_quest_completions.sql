{{ config(
    alias = 'quest_completions',
    partition_by = {"field": "block_date"},
    materialized = 'view',
            unique_key = ['block_time', 'tx_hash', 'nft_id']
    )
}}
--SELECT MIN(call_block_time) FROM optimism_quest_optimism.StarNFTV4_call_mint m
{% set project_start_date = '2022-09-13' %}

SELECT
    TIMESTAMP_TRUNC(call_block_time, day) AS block_date,
    account AS quester_address,
    call_tx_hash AS tx_hash,
    call_block_number AS block_number,
    call_block_time AS block_time,
    nft_id,
    contract_project,
    quest_project

FROM
    {{source('optimism_quest_optimism','StarNFTV4_call_mint')}} m
INNER JOIN {{ref('optimism_quests_optimism_nft_id_mapping')}} nft 
    ON cast(m.cid as STRING) = nft.nft_id

WHERE call_success = true
AND call_block_time >= cast( '{{project_start_date}}' as timestamp)
{% if is_incremental() %}
AND call_block_time >= date_trunc('day', CURRENT_TIMESTAMP() - interval '1 week')
{% endif %}