{{ config(
        alias ='revenue',
        partition_by = ['period'],
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido",
                                \'["pipistrella", "adcv", "zergil1397", "lido"]\') }}'
        )
}}
--https://dune.com/queries/2011922
--ref{{ 'lido_accounting_revenue' }}

with oracle_txns as (
    select
        evt_block_time as period,
        (CAST(posttotalpooledether as DOUBLE) - CAST(pretotalpooledether as DOUBLE)) as lido_rewards,
        evt_tx_hash
    from {{ source('lido_ethereum','LidoOracle_evt_PostTotalShares') }}
    order by 1 desc
),

protocol_fee as (
    select
        DATE_TRUNC('day', evt_block_time) as period,
        LEAD(DATE_TRUNC('day', evt_block_time), 1, NOW()) over (order by DATE_TRUNC('day', evt_block_time)) as next_period,
        CAST(feebasispoints as DOUBLE) / 10000 as points
    from {{ source('lido_ethereum','steth_evt_FeeSet') }}
),

protocol_fee_distribution as (
    select
        DATE_TRUNC('day', evt_block_time) as period,
        LEAD(DATE_TRUNC('day', evt_block_time), 1, NOW()) over (order by DATE_TRUNC('day', evt_block_time)) as next_period,
        CAST(insurancefeebasispoints as DOUBLE) / 10000 as insurance_points,
        CAST(operatorsfeebasispoints as DOUBLE) / 10000 as operators_points,
        CAST(treasuryfeebasispoints as DOUBLE) / 10000 as treasury_points
    from {{ source('lido_ethereum','steth_evt_FeeDistributionSet') }}
)


select
    oracle_txns.period as period,
    oracle_txns.evt_tx_hash,
    LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') as token,
    lido_rewards as total,
    protocol_fee.points as protocol_fee,
    protocol_fee_distribution.insurance_points as insurance_fee,
    protocol_fee_distribution.operators_points as operators_fee,
    protocol_fee_distribution.treasury_points as treasury_fee,
    (1 - protocol_fee.points) * lido_rewards as depositors_revenue,
    protocol_fee.points * protocol_fee_distribution.treasury_points * lido_rewards as treasury_revenue,
    protocol_fee.points * protocol_fee_distribution.insurance_points * lido_rewards as insurance_revenue,
    protocol_fee.points * protocol_fee_distribution.operators_points * lido_rewards as operators_revenue
from oracle_txns
left join protocol_fee on DATE_TRUNC('day', oracle_txns.period) >= protocol_fee.period and DATE_TRUNC('day', oracle_txns.period) < protocol_fee.next_period
left join protocol_fee_distribution on DATE_TRUNC('day', oracle_txns.period) >= protocol_fee_distribution.period and DATE_TRUNC('day', oracle_txns.period) < protocol_fee_distribution.next_period
order by 1, 2
