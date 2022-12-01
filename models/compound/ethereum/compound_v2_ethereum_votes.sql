{{ config(
    schema = 'compound_v2_ethereum',
    alias = 'votes',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'blockchain', 'project', 'version', 'tx_hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "compound_v2",
                                \'["soispoke"]\') }}'
    )
}}

{% set blockchain = 'ethereum' %}
{% set project = 'compound' %}
{% set project_version = 'v2' %}
{% set dao_name = 'DAO: Compound' %}
{% set dao_address = '0xc0da02939e1441f497fd74f78ce7decb17b66529' %}

WITH cte_sum_votes AS
(SELECT sum(votes / 1e18) AS sum_votes,
        proposalId
FROM {{ source('compound_v2_ethereum', 'GovernorBravoDelegate_evt_VoteCast') }}
GROUP BY proposalId)

SELECT
    '{{ blockchain }}' AS blockchain,
    '{{project}}' AS project,
    '{{project_version}}' AS version,
    vc.evt_block_time AS block_time,
    date_trunc('DAY', vc.evt_block_time) AS block_date,
    vc.evt_tx_hash AS tx_hash,
    '{{dao_name}}' AS dao_name,
    '{{dao_address}}' AS dao_address,
    vc.proposalId AS proposal_id,
    vc.votes / 1e18 AS votes,
    (votes / 1e18) * (100) / (csv.sum_votes) AS votes_share,
    p.symbol AS token_symbol,
    p.contract_address AS token_address,
    vc.votes / 1e18 * p.price AS votes_value_usd,
    vc.voter AS voter_address,
    CASE WHEN vc.support = 0 THEN 'against'
         WHEN vc.support = 1 THEN 'for'
         WHEN vc.support = 2 THEN 'abstain'
         END AS support,
    vc.reason
FROM {{ source('compound_v2_ethereum', 'GovernorBravoDelegate_evt_VoteCast') }} vc
LEFT JOIN cte_sum_votes csv ON vc.proposalId = csv.proposalId
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', evt_block_time)
    AND p.symbol = 'COMP'
    AND p.blockchain ='ethereum'
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE evt_block_time > (SELECT max(block_time) FROM {{ this }})
{% endif %}