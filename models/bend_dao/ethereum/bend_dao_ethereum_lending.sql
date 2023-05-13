{{ config(
    alias = 'lending',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'evt_type', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "bend_dao",
                                \'["Henrystats"]\') }}'
    )
}}

{%- set project_start_date = '2022-03-21' %}

WITH

borrow_events AS (
    SELECT
        contract_address,
        evt_block_time,
        evt_tx_hash,
        evt_block_number,
        evt_index,
        nfttokenid AS token_id,
        nftasset AS nft_contract_address,
        'Borrow' AS evt_type,
        onbehalfof AS address,
        amount AS amount_raw,
        reserve AS collateral_currency_contract
    FROM
        {{ source('bend_ethereum', 'LendingPool_evt_Borrow') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

repay_events AS (
    SELECT
        contract_address,
        evt_block_time,
        evt_tx_hash,
        evt_block_number,
        evt_index,
        nfttokenid AS token_id,
        nftasset AS nft_contract_address,
        'Repay' AS evt_type,
        borrower AS address,
        amount AS amount_raw,
        reserve AS collateral_currency_contract
    FROM
        {{ source('bend_ethereum', 'LendingPool_evt_Repay') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

all_events AS (
    SELECT * FROM borrow_events

    UNION ALL

    SELECT * FROM repay_events
)

SELECT
    'ethereum' AS blockchain,
    'bend_dao' AS project,
    '1' AS version,
    date_trunc('day', ae.evt_block_time) AS block_date,
    ae.evt_block_time AS block_time,
    ae.evt_block_number AS block_number,
    ae.token_id,
    nft_token.name AS collection,
    p.price * (ae.amount_raw / POWER(10, collateral_currency.decimals)) AS amount_usd,
    nft_token.standard AS token_standard,
    ae.evt_type,
    ae.address,
    ae.amount_raw / POWER(10, collateral_currency.decimals) AS amount_original,
    CAST(ae.amount_raw AS decimal(38, 0)) AS amount_raw,
    collateral_currency.symbol AS collateral_currency_symbol,
    ae.collateral_currency_contract,
    ae.nft_contract_address,
    ae.contract_address AS project_contract_address,
    ae.evt_tx_hash AS tx_hash,
    et.from AS tx_from,
    et.to AS tx_to,
    ae.evt_index
FROM
    all_events AS ae
INNER JOIN
    {{ source('ethereum','transactions') }} AS et
    ON
        et.block_time = ae.evt_block_time
        AND et.hash = ae.evt_tx_hash
        {% if not is_incremental() %}
    AND et.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND et.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN
    {{ ref('tokens_ethereum_nft') }} AS nft_token
    ON nft_token.contract_address = ae.nft_contract_address
LEFT JOIN
    {{ ref('tokens_ethereum_erc20') }} AS collateral_currency
    ON collateral_currency.contract_address = ae.collateral_currency_contract
LEFT JOIN
    {{ source('prices', 'usd') }} AS p
    ON
        p.minute = date_trunc('minute', ae.evt_block_time)
        AND p.contract_address = ae.collateral_currency_contract
        AND p.blockchain = 'ethereum'
        {% if not is_incremental() %}
    AND p.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND p.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
