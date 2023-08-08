{{ config(
        alias ='revenue',
        partition_by = {"field": "period"},
        materialized = 'view'
        )
}}
--https://dune.com/queries/2011922
--ref{{'lido_accounting_revenue'}}

with oracle_txns AS ( 
    SELECT
        evt_block_time AS period,
        (CAST(postTotalPooledEther AS FLOAT64)-CAST(preTotalPooledEther AS FLOAT64)) lido_rewards,
        evt_tx_hash
    FROM {{source('lido_ethereum','LidoOracle_evt_PostTotalShares')}}
    ORDER BY 1 DESC
),

protocol_fee AS (
    SELECT 
        TIMESTAMP_TRUNC(evt_block_time, day) AS period, 
        LEAD(TIMESTAMP_TRUNC(evt_block_time, day), 1, CURRENT_TIMESTAMP()) OVER (ORDER BY TIMESTAMP_TRUNC(evt_block_time, day)) AS next_period,
        CAST(feeBasisPoints AS FLOAT64)/10000 AS points
    FROM {{source('lido_ethereum','steth_evt_FeeSet')}}
),

protocol_fee_distribution AS (
    SELECT 
        TIMESTAMP_TRUNC(evt_block_time, day) AS period, 
        LEAD(TIMESTAMP_TRUNC(evt_block_time, day), 1, CURRENT_TIMESTAMP()) OVER (ORDER BY TIMESTAMP_TRUNC(evt_block_time, day)) AS next_period,
        CAST(insuranceFeeBasisPoints AS FLOAT64)/10000 AS insurance_points,
        CAST(operatorsFeeBasisPoints AS FLOAT64)/10000 AS operators_points,
        CAST(treasuryFeeBasisPoints AS FLOAT64)/10000 AS treasury_points
    FROM {{source('lido_ethereum','steth_evt_FeeDistributionSet')}}
)


    SELECT  
        oracle_txns.period AS period, 
        oracle_txns.evt_tx_hash,
        LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') AS token,
        lido_rewards AS total,
        protocol_fee.points AS protocol_fee,
        protocol_fee_distribution.insurance_points AS insurance_fee,
        protocol_fee_distribution.operators_points AS operators_fee,
        protocol_fee_distribution.treasury_points AS treasury_fee,
        (1 - protocol_fee.points)*lido_rewards AS depositors_revenue,
        protocol_fee.points*protocol_fee_distribution.treasury_points*lido_rewards AS treasury_revenue,
        protocol_fee.points*protocol_fee_distribution.insurance_points*lido_rewards AS insurance_revenue,
        protocol_fee.points*protocol_fee_distribution.operators_points*lido_rewards AS operators_revenue
    FROM oracle_txns
    LEFT JOIN protocol_fee ON TIMESTAMP_TRUNC(oracle_txns.period, day) >= protocol_fee.period AND TIMESTAMP_TRUNC(oracle_txns.period, day) < protocol_fee.next_period
    LEFT JOIN protocol_fee_distribution ON TIMESTAMP_TRUNC(oracle_txns.period, day) >= protocol_fee_distribution.period AND TIMESTAMP_TRUNC(oracle_txns.period, day) < protocol_fee_distribution.next_period
    ORDER BY 1,2