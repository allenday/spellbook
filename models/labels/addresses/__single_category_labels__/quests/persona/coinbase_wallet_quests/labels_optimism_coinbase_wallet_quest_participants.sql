{{
    config(
        alias='coinbase_wallet_quest_participants',
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
        'optimism' as blockchain
    from {{ ref('coinbase_wallet_quests_optimism_rewards_transfers') }}
    group by 1
)

select
    blockchain,
    quester_address as address,
    'Coinbase Wallet Quest Participant' as name,
    'quests' as category,
    'msilb7' as contributor,
    'query' as source,
    timestamp('2023-03-11') as created_at,
    now() as updated_at,
    'coinbase_wallet_quest_participants' as model_name,
    'persona' as label_type
from
    questers
