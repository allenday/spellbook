{{ config(
    alias = 'node_names',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['node'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "ens",
                            \'["0xRob"]\') }}'
    )
}}

-- because we don't have the keccak namehash function available in v2
-- we do a little sketchy event matching to get the node <> name relationships
-- basically this takes the last AddrChanged event in the same tx preceding a NameRegistred event to link the node AND the name
-- ONLY works FOR base ENS names (.eth , no subdomains)
WITH registrations AS (
    SELECT
        label AS label_hash
        , name AS label_name
        , evt_block_number AS block_number
        , evt_block_time AS block_time
        , evt_tx_hash AS tx_hash
        , evt_index
    FROM {{ ref('ens_view_registrations') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - INTERVAL '1 week')
    {% endif %}
)

, node_info AS (
    SELECT
        a AS address
        , node
        , evt_block_number AS block_number
        , evt_block_time AS block_time
        , evt_tx_hash AS tx_hash
        , evt_index
    FROM
        {{ source('ethereumnameservice_ethereum', 'PublicResolver_evt_AddrChanged') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - INTERVAL '1 week')
    {% endif %}
)

-- here's the sketchy matching
, matching AS (
    SELECT *
    FROM (
        SELECT
            *
            , row_number() OVER (
                PARTITION BY node ORDER BY block_time DESC, evt_index DESC
            ) AS ordering2
        FROM (
            SELECT
                registrations.*
                , node_info.address
                , node_info.node
                , row_number() OVER (
                    PARTITION BY
                        registrations.tx_hash
                    ORDER BY (registrations.evt_index - node_info.evt_index) ASC
                ) AS ordering
            FROM registrations
            INNER JOIN node_info
                ON registrations.block_number = node_info.block_number
                    AND registrations.tx_hash = node_info.tx_hash
                    --register event comes after node event
                    AND registrations.evt_index > node_info.evt_index
        )
        WHERE ordering = 1
    )
    WHERE ordering2 = 1
)

SELECT
    node
    , label_name
    , label_hash
    , address AS initial_address
    , tx_hash
    , block_number
    , block_time
    , evt_index
    , concat(label_name, ".eth") AS name
FROM matching
