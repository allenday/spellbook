{{ config(
    schema = 'uniswap_v3_ethereum',
    alias = 'proposals',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_at', 'blockchain', 'project', 'version', 'tx_hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "uniswap_v3",
                                \'["soispoke"]\') }}'
    )
}}

{% set blockchain = 'ethereum' %}
{% set project = 'uniswap' %}
{% set project_version = 'v3' %}
{% set dao_name = 'DAO: Uniswap' %}
{% set dao_address = '0x408ed6354d4973f66138c91495f2f2fcbd8724c3' %}

with cte_support as (
    select
        voter as voter,
        case when support = 0 then sum(votes / 1e18) else 0 end as votes_against,
        case when support = 1 then sum(votes / 1e18) else 0 end as votes_for,
        case when support = 2 then sum(votes / 1e18) else 0 end as votes_abstain,
        proposalid
    from {{ source('uniswap_v3_ethereum', 'GovernorBravoDelegate_evt_VoteCast') }}
    group by support, proposalid, voter
),

cte_sum_votes as (
    select
        COUNT(distinct voter) as number_of_voters,
        SUM(votes_for) as votes_for,
        SUM(votes_against) as votes_against,
        SUM(votes_abstain) as votes_abstain,
        SUM(votes_for) + SUM(votes_against) + SUM(votes_abstain) as votes_total,
        proposalid
    from cte_support
    group by proposalid
)

select distinct
    '{{ blockchain }}' as blockchain,
    '{{ project }}' as project,
    '{{ project_version }}' as version,
    pcr.evt_block_time as created_at,
    date_trunc('DAY', pcr.evt_block_time) as block_date,
    pcr.evt_tx_hash as tx_hash, -- Proposal Created tx hash
    '{{ dao_name }}' as dao_name,
    '{{ dao_address }}' as dao_address,
    proposer,
    pcr.id as proposal_id,
    csv.votes_for,
    csv.votes_against,
    csv.votes_abstain,
    csv.votes_total,
    csv.number_of_voters,
    csv.votes_total / 1e9 * 100 as participation, -- Total votes / Total supply (1B for Uniswap)
    pcr.startblock as start_block,
    pcr.endblock as end_block,
    case
        when pex.id is not null and now() > pex.evt_block_time then 'Executed'
        when pca.id is not null and now() > pca.evt_block_time then 'Canceled'
        when pcr.startblock < pcr.evt_block_number < pcr.endblock then 'Active'
        when now() > pqu.evt_block_time and startblock > pcr.evt_block_number then 'Queued'
        else 'Defeated'
    end as status,
    description as description
from {{ source('uniswap_v3_ethereum', 'GovernorBravoDelegate_evt_ProposalCreated') }} as pcr
left join cte_sum_votes as csv on csv.proposalid = pcr.id
left join {{ source('uniswap_v3_ethereum', 'GovernorBravoDelegate_evt_ProposalCanceled') }} as pca on pca.id = pcr.id
left join {{ source('uniswap_v3_ethereum', 'GovernorBravoDelegate_evt_ProposalExecuted') }} as pex on pex.id = pcr.id
left join {{ source('uniswap_v3_ethereum', 'GovernorBravoDelegate_evt_ProposalQueued') }} as pqu on pex.id = pcr.id
{% if is_incremental() %}
    where pcr.evt_block_time > (select max(created_at) from {{ this }})
{% endif %}
