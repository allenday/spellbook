{{ config(
    schema = 'dydx_ethereum',
    alias = 'votes',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'voter_address'],
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

WITH cte_sum_votes AS (
    SELECT
        sum(votingpower / 1e18) AS sum_votes,
        id
    FROM {{ source('dydx_protocol_ethereum', 'DydxGovernor_evt_VoteEmitted') }}
    GROUP BY id
)

SELECT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    cast(NULL AS string) AS version,
    vc.evt_block_time AS block_time,
    date_trunc('DAY', vc.evt_block_time) AS block_date,
    vc.evt_tx_hash AS tx_hash,
    '{{ dao_name }}' AS dao_name,
    '{{ dao_address }}' AS dao_address,
    vc.id AS proposal_id,
    vc.votingpower / 1e18 AS votes,
    (votingpower / 1e18) * (100) / (csv.sum_votes) AS votes_share,
    p.symbol AS token_symbol,
    p.contract_address AS token_address,
    vc.votingpower / 1e18 * p.price AS votes_value_usd,
    vc.voter AS voter_address,
    CASE
        WHEN vc.support = 0 THEN 'against'
        WHEN vc.support = 1 THEN 'for'
        WHEN vc.support = 2 THEN 'abstain'
    END AS support,
    cast(NULL AS string) AS reason
FROM {{ source('dydx_protocol_ethereum', 'DydxGovernor_evt_VoteEmitted') }} AS vc
LEFT JOIN cte_sum_votes AS csv ON vc.id = csv.id
LEFT JOIN {{ source('prices', 'usd') }} AS p
    ON
        p.minute = date_trunc('minute', evt_block_time)
        AND p.symbol = 'DYDX'
        AND p.blockchain = 'ethereum'
        {% if is_incremental() %}
            AND p.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
{% if is_incremental() %}
    WHERE evt_block_time > (SELECT max(block_time) FROM {{ this }})
{% endif %}
