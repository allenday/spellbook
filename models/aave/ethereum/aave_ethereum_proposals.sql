{{ config(
    schema = 'aave_ethereum',
    alias = 'proposals',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_at', 'blockchain', 'project', 'version', 'tx_hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "aave",
                                \'["soispoke"]\') }}'
    )
}}

{% set blockchain = 'ethereum' %}
{% set project = 'aave' %}
{% set dao_name = 'DAO: AAVE' %}
{% set dao_address = '0xec568fffba86c094cf06b22134b23074dfe2252c' %}

with cte_support AS (SELECT
        voter AS voter,
        CASE WHEN support = 0 THEN sum(votingPower / 1e18) ELSE 0 END AS votes_against,
        CASE WHEN support = 1 THEN sum(votingPower / 1e18) ELSE 0 END AS votes_for,
        CASE WHEN support = 2 THEN sum(votingPower / 1e18) ELSE 0 END AS votes_abstain,
        id
    FROM {{ source('aave_ethereum', 'AaveGovernanceV2_evt_VoteEmitted') }}
    GROUP BY support, id, voter),

cte_sum_votes AS (
    SELECT COUNT(DISTINCT voter) AS number_of_voters,
        SUM(votes_for) AS votes_for,
        SUM(votes_against) AS votes_against,
        SUM(votes_abstain) AS votes_abstain,
        SUM(votes_for) + SUM(votes_against) + SUM(votes_abstain) AS votes_total,
        id
    FROM cte_support
    GROUP BY id)

SELECT DISTINCT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    cast(NULL AS STRING) AS version,
    pcr.evt_block_time AS created_at,
    date_trunc('DAY', pcr.evt_block_time) AS block_date,
    pcr.evt_tx_hash AS tx_hash, -- Proposal Created tx hash
    '{{ dao_name }}' AS dao_name,
    '{{ dao_address }}' AS dao_address,
    creator AS proposer,
    pcr.id AS proposal_id,
    csv.votes_for,
    csv.votes_against,
    csv.votes_abstain,
    csv.votes_total,
    csv.number_of_voters,
    csv.votes_total / 1e9 * 100 AS participation, -- Total votes / Total supply (1B FOR Uniswap)
    pcr.startBlock AS start_block,
    pcr.endBlock AS end_block,
    CASE
        WHEN pex.id is NOT NULL AND now() > pex.evt_block_time THEN 'Executed'
        WHEN pca.id is NOT NULL AND now() > pca.evt_block_time THEN 'Canceled'
        WHEN pcr.startBlock < pcr.evt_block_number < pcr.endBlock THEN 'Active'
        WHEN now() > pqu.evt_block_time AND startBlock > pcr.evt_block_number THEN 'Queued'
        ELSE 'Defeated' END AS status,
    cast(NULL AS STRING) AS description
FROM  {{ source('aave_ethereum', 'AaveGovernanceV2_evt_ProposalCreated') }} AS pcr
LEFT JOIN cte_sum_votes AS csv ON csv.id = pcr.id
LEFT JOIN {{ source('aave_ethereum', 'AaveGovernanceV2_evt_ProposalCanceled') }} AS pca ON pca.id = pcr.id
LEFT JOIN {{ source('aave_ethereum', 'AaveGovernanceV2_evt_ProposalExecuted') }} AS pex ON pex.id = pcr.id
LEFT JOIN {{ source('aave_ethereum', 'AaveGovernanceV2_evt_ProposalQueued') }} AS pqu ON pex.id = pcr.id
{% if is_incremental() %}
    WHERE pcr.evt_block_time > (SELECT max(created_at) FROM {{ this }})
{% endif %}