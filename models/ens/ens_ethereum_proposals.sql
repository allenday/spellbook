{{ config(
    schema = 'ens_ethereum',
    alias = 'proposals',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_at', 'blockchain', 'project', 'version', 'tx_hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "ens",
                                \'["soispoke"]\') }}'
    )
}}

{% set blockchain = 'ethereum' %}
{% set project = 'ens' %}
{% set dao_name = 'DAO: ENS' %}
{% set dao_address = '0x323a76393544d5ecca80cd6ef2a560c6a395b7e3' %}

WITH cte_support AS (SELECT
        voter AS voter
        , proposalId
        , CASE
            WHEN support = 0 THEN sum(weight / 1e18) ELSE 0
        END AS votes_against
        , CASE WHEN support = 1 THEN sum(weight / 1e18) ELSE 0 END AS votes_for
        , CASE
            WHEN support = 2 THEN sum(weight / 1e18) ELSE 0
        END AS votes_abstain
    FROM
        {{ source('ethereumnameservice_ethereum', 'ENSGovernor_evt_VoteCast') }}
    GROUP BY support, proposalId, voter
)

, cte_sum_votes AS (
    SELECT
        proposalId
        , count(DISTINCT voter) AS number_of_voters
        , sum(votes_for) AS votes_for
        , sum(votes_against) AS votes_against
        , sum(votes_abstain) AS votes_abstain
        , sum(
            votes_for
        ) + sum(votes_against) + sum(votes_abstain) AS votes_total
    FROM cte_support
    GROUP BY proposalId
)

SELECT DISTINCT
    '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , cast(NULL AS STRING) AS version
    , pcr.evt_block_time AS created_at
    , date_trunc('DAY', pcr.evt_block_time) AS block_date
    , pcr.evt_tx_hash AS tx_hash -- Proposal Created tx hash
    , '{{ dao_name }}' AS dao_name
    , '{{ dao_address }}' AS dao_address
    , proposer
    , pcr.proposalId AS proposal_id
    , cte_sum_votes.votes_for
    , cte_sum_votes.votes_against
    , cte_sum_votes.votes_abstain
    , cte_sum_votes.votes_total
    , cte_sum_votes.number_of_voters
    -- Total votes / Total supply (1B FOR Uniswap)
    , cte_sum_votes.votes_total / 1e9 * 100 AS participation
    , pcr.startBlock AS start_block
    , pcr.endBlock AS end_block
    , CASE
        WHEN
            pex.proposalId IS NOT NULL AND now() > pex.evt_block_time THEN 'Executed'
        WHEN
            pca.proposalId IS NOT NULL AND now() > pca.evt_block_time THEN 'Canceled'
        WHEN pcr.startBlock < pcr.evt_block_number < pcr.endBlock THEN 'Active'
        WHEN
            now() > pqu.evt_block_time AND startBlock > pcr.evt_block_number THEN 'Queued'
        ELSE 'Defeated' END AS status
    , description
FROM
    {{ source('ethereumnameservice_ethereum', 'ENSGovernor_evt_ProposalCreated') }} AS pcr
LEFT JOIN cte_sum_votes ON cte_sum_votes.proposalId = pcr.proposalId
LEFT JOIN
    {{ source('ethereumnameservice_ethereum', 'ENSGovernor_evt_ProposalCanceled') }} AS pca ON
        pca.proposalId = pcr.proposalId
LEFT JOIN
    {{ source('ethereumnameservice_ethereum', 'ENSGovernor_evt_ProposalExecuted') }} AS pex ON
        pex.proposalId = pcr.proposalId
LEFT JOIN
    {{ source('ethereumnameservice_ethereum', 'ENSGovernor_evt_ProposalQueued') }} AS pqu ON
        pex.proposalId = pcr.proposalId
{% if is_incremental() %}
    WHERE pcr.evt_block_time > (SELECT max(created_at) FROM {{ this }})
{% endif %}
