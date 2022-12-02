{{ config(
    alias = 'resolver_records',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['node', 'block_time', 'evt_index'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "ens",
                            \'["0xRob"]\') }}'
    )
}}

WITH resolver_records AS (
    SELECT
        a AS address
        , node
        , evt_block_time AS block_time
        , evt_tx_hash AS tx_hash
        , evt_index
    FROM {{ source('ethereumnameservice_ethereum', 'PublicResolver_evt_AddrChanged') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - INTERVAL '1 week')
    {% endif %}
)

SELECT
    n.name
    , resolver_records.address
    , resolver_records.node
    , resolver_records.block_time
    , resolver_records.tx_hash
    , resolver_records.evt_index
FROM resolver_records
INNER JOIN {{ ref('ens_node_names') }} AS n
    ON resolver_records.node = n.node
