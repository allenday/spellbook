{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "optimism_attestationstation",
                                \'["chuxin"]\') }}'
    )
}}
SELECT
    *,
    concat_ws(', ', val) AS val_string

FROM (
    SELECT
        date_trunc('day', evt_block_time) AS block_date,
        evt_tx_hash AS tx_hash,
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        evt_index,
        about AS recipient,
        creator AS issuer,
        contract_address,
        key AS key_raw
        ,
        REGEXP_REPLACE(--Replace invisible characters
            decode(
                unhex(
                    if(
                        substring(key, 1, 6) IN ('0xab7e', '0x9e43'), --Handle for Clique
                        hex(key),
                        substring(key, 3)
                    )
                ),
                'utf8'
            ),
            '[^\x20-\x7E]', '')
            AS key,

        val AS val_raw,

        split(
            REGEXP_REPLACE(--Replace invisible characters
                CASE
                    WHEN cast(REGEXP_REPLACE(unhex(substring(val, 3)), '[^\x20-\x7E]', '') AS varchar(100)) != ''
                        THEN cast(unhex(substring(val, 3)) AS varchar(100))
                    ELSE cast(bytea2numeric_v3(substring(val, 3)) AS varchar(100))
                END,
                '[^\x20-\x7E]', ''
            ),
            ','
        ) AS val,


        bytea2numeric_v3(substring(val, 3)) AS val_byte2numeric

    FROM {{ source('attestationstation_optimism','AttestationStation_evt_AttestationCreated') }}
    WHERE
        true
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
) AS a
