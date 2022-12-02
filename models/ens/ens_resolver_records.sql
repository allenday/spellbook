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

with resolver_records AS (
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
    , r.address
    , r.node
    , r.block_time
    , r.tx_hash
    , r.evt_index
FROM resolver_records AS r
INNER JOIN {{ ref('ens_node_names')}} AS n
ON r.node = n.node
