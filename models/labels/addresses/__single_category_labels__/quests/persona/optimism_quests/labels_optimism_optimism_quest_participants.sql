{{
    config(
        alias='optimism_quest_participants',
        post_hook='{{ expose_spells(\'["optimism"]\', 
        "sector", 
        "labels", 
        \'["msilb7"]\') }}'
    )
}}

with
questers as (
    select
        quester_address,
        'optimism' as blockchain,
        COUNT(*) as num_quests_completed
    from {{ ref('optimism_quests_optimism_quest_completions') }}
    group by 1, 2
)

select
    blockchain,
    quester_address as address,
    'Optimism Quests Participant' as name,
    'quests' as category,
    'msilb7' as contributor,
    'query' as source,
    timestamp('2023-03-11') as created_at,
    now() as updated_at,
    'optimism_quest_participants' as model_name,
    'persona' as label_type
from
    questers

union all

select
    blockchain,
    quester_address as address,
    'Optimism Quests - '
    || case
        when num_quests_completed >= 10 then 'Tier 3'
        when num_quests_completed >= 7 then 'Tier 2'
        else 'Tier 1'
    end as name,
    'quests' as category,
    'msilb7' as contributor,
    'query' as source,
    timestamp('2023-03-11') as created_at,
    now() as updated_at,
    'optimism_quest_participants' as model_name,
    'persona' as label_type
from
    questers
where num_quests_completed >= 4
