{{ config(
    schema = 'dydx_ethereum',
    alias = 'proposals',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'tx_hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "dydx",
                                \'["ivigamberdiev"]\') }}'
    )
}}

{% set blockchain = 'ethereum' %}
{% set project = 'dydx' %}
{% set dao_name = 'DAO: dYdX' %}
{% set dao_address = '0x7e9b1672616ff6d6629ef2879419aae79a9018d2' %}

with cte_latest_block as (
    select MAX(b.number) as latest_block
    from {{ source('ethereum','blocks') }} as b
),

cte_support as (
    select
        voter as voter,
        case when support = 0 then sum(votingpower / 1e18) else 0 end as votes_against,
        case when support = 1 then sum(votingpower / 1e18) else 0 end as votes_for,
        case when support = 2 then sum(votingpower / 1e18) else 0 end as votes_abstain,
        id
    from {{ source('dydx_protocol_ethereum', 'DydxGovernor_evt_VoteEmitted') }}
    group by support, id, voter
),

cte_sum_votes as (
    select
        COUNT(distinct voter) as number_of_voters,
        SUM(votes_for) as votes_for,
        SUM(votes_against) as votes_against,
        SUM(votes_abstain) as votes_abstain,
        SUM(votes_for) + SUM(votes_against) + SUM(votes_abstain) as votes_total,
        id
    from cte_support
    group by id
)

select distinct
    '{{ blockchain }}' as blockchain,
    '{{ project }}' as project,
    cast(NULL as string) as version,
    pcr.evt_block_time as created_at,
    date_trunc('DAY', pcr.evt_block_time) as block_date,
    pcr.evt_tx_hash as tx_hash, -- Proposal Created tx hash
    '{{ dao_name }}' as dao_name,
    '{{ dao_address }}' as dao_address,
    creator as proposer,
    pcr.id as proposal_id,
    csv.votes_for,
    csv.votes_against,
    csv.votes_abstain,
    csv.votes_total,
    csv.number_of_voters,
    csv.votes_total / 1e9 * 100 as participation, -- Total votes / Total supply (1B for dYdX)
    pcr.startblock as start_block,
    pcr.endblock as end_block,
    case
        when pex.id is not null and now() > pex.evt_block_time then 'Executed'
        when pca.id is not null and now() > pca.evt_block_time then 'Canceled'
        when (select latest_block from cte_latest_block) <= pcr.startblock then 'Pending'
        when (select latest_block from cte_latest_block) <= pcr.endblock then 'Active'
        when pqu.id is not null and now() > pqu.evt_block_time and now() < CAST(CAST(pqu.executiontime as numeric) as timestamp) then 'Queued'
        else 'Defeated'
    end as status,
    cast(NULL as string) as description
from {{ source('dydx_protocol_ethereum', 'DydxGovernor_evt_ProposalCreated') }} as pcr
left join cte_sum_votes as csv on csv.id = pcr.id
left join {{ source('dydx_protocol_ethereum', 'DydxGovernor_evt_ProposalCanceled') }} as pca on pca.id = pcr.id
left join {{ source('dydx_protocol_ethereum', 'DydxGovernor_evt_ProposalExecuted') }} as pex on pex.id = pcr.id
left join {{ source('dydx_protocol_ethereum', 'DydxGovernor_evt_ProposalQueued') }} as pqu on pqu.id = pcr.id
{% if is_incremental() %}
    where pcr.evt_block_time > (select max(created_at) from {{ this }})
{% endif %}
