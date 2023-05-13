{{ config(
        alias = 'pool_creations',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "sudoswap",
                                    \'["niftytable","0xRob"]\') }}'
        )
}}

{% set project_start_date = '2022-04-23' %}
{% set linear_bonding_address = '0x5b6ac51d9b1cede0068a1b26533cace807f883ee' %}
{% set exponential_bonding_address = '0x432f962d8209781da23fb37b6b59ee15de7d9841' %}
{% set xyk_bonding_address = '0x7942e264e21c5e6cbba45fe50785a15d3beb1da0' %}

WITH
pool_creations AS (
    SELECT
        output_pair AS pool_address,
        _nft AS nft_contract_address,
        tx.from AS creator_address,
        CASE
            WHEN lower(_bondingcurve) = '{{ linear_bonding_address }}' THEN 'linear'
            WHEN lower(_bondingcurve) = '{{ exponential_bonding_address }}' THEN 'exponential'
            WHEN lower(_bondingcurve) = '{{ xyk_bonding_address }}' THEN 'xyk'
            ELSE 'other'
        END AS bonding_curve,
        CASE
            WHEN _pooltype = 0 THEN 'token'
            WHEN _pooltype = 1 THEN 'nft'
            WHEN _pooltype = 2 THEN 'trade'
        END AS pool_type,
        _spotprice / 1e18 AS spot_price,
        _delta / 1e18 AS delta,
        _fee AS pool_fee,
        coalesce(cardinality(_initialnftids), 0) AS initial_nft_balance,
        coalesce(tx.value / 1e18, 0) AS initial_eth_balance,
        contract_address AS pool_factory,
        call_block_time AS creation_block_time,
        call_tx_hash AS creation_tx_hash
    FROM
        {{ source('sudo_amm_ethereum','LSSVMPairFactory_call_createPairETH') }} AS cre
    INNER JOIN {{ source('ethereum','transactions') }} AS tx
        ON
            tx.block_time = cre.call_block_time
            AND tx.hash = cre.call_tx_hash
            {% if not is_incremental() %}
        AND tx.block_time >= '{{ project_start_date }}'
        {% endif %}
            {% if is_incremental() %}
                AND tx.block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    WHERE
        call_success
)

SELECT * FROM pool_creations;
