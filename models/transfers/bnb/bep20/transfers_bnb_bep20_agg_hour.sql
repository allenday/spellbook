{{ config(
        alias ='bep20_agg_hour',
        partition_by = ['hour'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key=['wallet_address', 'token_address', 'hour'],
        post_hook='{{ expose_spells(\'["bnb"]\',
                                        "sector",
                                        "transfers",
                                        \'["hosuke"]\') }}'
        )
}}

WITH
sent_transfers AS (
    SELECT
        `to` AS wallet_address
        , contract_address AS token_address
        , evt_block_time
        , value AS amount_raw
    FROM
        {{ source('erc20_bnb', 'evt_Transfer') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)
,
received_transfers AS (
    SELECT
        `from` AS wallet_address
        , contract_address AS token_address
        , evt_block_time
        , - value AS amount_raw
    FROM
        {{ source('erc20_bnb', 'evt_Transfer') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)
,
deposited_wbnb AS (
    SELECT
        dst AS wallet_address
        , contract_address AS token_address
        , evt_block_time
        , wad AS amount_raw
    FROM
        {{ source('bnb_bnb', 'WBNB_evt_Deposit') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)
,
withdrawn_wbnb AS (
    SELECT
        src AS wallet_address
        , contract_address AS token_address
        , evt_block_time
        , - wad AS amount_raw
    FROM
        {{ source('bnb_bnb', 'WBNB_evt_Withdrawal') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)
,
transfers_bnb_bep20 AS (
    SELECT
        wallet_address
        , token_address
        , evt_block_time
        , amount_raw
    FROM sent_transfers

    UNION

    SELECT
        wallet_address
        , token_address
        , evt_block_time
        , amount_raw
    FROM received_transfers

    UNION

    SELECT
        wallet_address
        , token_address
        , evt_block_time
        , amount_raw
    FROM deposited_wbnb

    UNION

    SELECT
        wallet_address
        , token_address
        , evt_block_time
        , amount_raw
    FROM withdrawn_wbnb
)

SELECT
    'bnb' AS blockchain
    , date_trunc('hour', transfers_bnb_bep20.evt_block_time) AS hour
    , transfers_bnb_bep20.wallet_address
    , transfers_bnb_bep20.token_address
    , t.symbol
    , sum(transfers_bnb_bep20.amount_raw) AS amount_raw
    , sum(transfers_bnb_bep20.amount_raw / power(10, t.decimals)) AS amount
FROM transfers_bnb_bep20
LEFT JOIN
    {{ ref('tokens_bnb_bep20') }} AS t ON
        t.contract_address = transfers_bnb_bep20.token_address
GROUP BY 1, 2, 3, 4, 5
