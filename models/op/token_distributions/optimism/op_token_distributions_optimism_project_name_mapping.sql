
{{ config(
    alias = 'project_name_mapping'
    )
}}

SELECT
        proposal_name, project_name

FROM UNNEST(ARRAY<STRUCT<proposal_name STRING,project_name STRING>> [STRUCT('xToken Terminal / Gamma Strategies', 'xToken'),
STRUCT('Rainbow Wallet', 'Rainbow'),
STRUCT('Karma 1', 'Karma'),
STRUCT('Karma 2', 'Karma'),
STRUCT('Safe', 'Gnosis Safe'),
STRUCT('Okex', 'OKX'),
STRUCT('Overtime Markets', 'Thales'),
STRUCT('Quests on Coinbase Wallet - Quest #1 DEX Swap','Quests on Coinbase Wallet'),
STRUCT('Quests on Coinbase Wallet - Quest #2 Delegation','Quests on Coinbase Wallet'),
STRUCT('Uniswap V3','Uniswap'),
STRUCT('SushiSwap', 'Sushi'),
STRUCT('Karma delegate registry', 'Karma'),
STRUCT('Rabbit Hole','Rabbithole')])