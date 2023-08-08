{{ config(
    schema = 'compound_v2_ethereum',
    alias = 'votes',
    partition_by = {"field": "block_date"},
    materialized = 'view',
            unique_key = ['block_time', 'blockchain', 'project', 'version', 'tx_hash']
    )
}}

{% set blockchain = 'ethereum' %}
{% set project = 'compound' %}
{% set project_version = 'v2' %}
{% set dao_name = 'DAO: Compound' %}
{% set dao_address = '0xc0da02939e1441f497fd74f78ce7decb17b66529' %}

WITH cte_sum_votes as 
(SELECT sum(votes/1e18) as sum_votes, 
        proposalId
FROM {{ source('compound_v2_ethereum', 'GovernorBravoDelegate_evt_VoteCast') }}
GROUP BY proposalId)

SELECT 
    '{{blockchain}}' as blockchain,
    '{{project}}' as project,
    '{{project_version}}' as version,
    vc.evt_block_time as block_time,
    TIMESTAMP_TRUNC(vc.evt_block_time, DAY) AS block_date,
    vc.evt_tx_hash as tx_hash,
    '{{dao_name}}' as dao_name,
    '{{dao_address}}' as dao_address,
    vc.proposalId as proposal_id,
    vc.votes/1e18 as votes,
    (votes/1e18) * (100) / (csv.sum_votes) as votes_share,
    p.symbol as token_symbol,
    p.contract_address as token_address, 
    vc.votes/1e18 * p.price as votes_value_usd,
    vc.voter as voter_address,
    CASE WHEN vc.support = 0 THEN 'against'
         WHEN vc.support = 1 THEN 'for'
         WHEN vc.support = 2 THEN 'abstain'
         END AS support,
    vc.reason
FROM {{ source('compound_v2_ethereum', 'GovernorBravoDelegate_evt_VoteCast') }} vc
LEFT JOIN cte_sum_votes csv ON vc.proposalId = csv.proposalId
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = TIMESTAMP_TRUNC(evt_block_time, minute)
    AND p.symbol = 'COMP'
    AND p.blockchain ='ethereum'
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE evt_block_time > (select max(block_time) from {{ this }})
{% endif %}