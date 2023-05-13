{{ config(alias='op_retropgf',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke", "msilb7"]\') }}')
}}

SELECT
    blockchain,
    submitter_address AS address,
    'OP RetroPGF ' || round_name || ' Recipient - Submitter' AS name,
    'op_retropgf' AS category,
    'msilb7' AS contributor,
    'query' AS source,
    date('2023-03-30') AS created_at,
    now() AS updated_at,
    'op_retropgf_recipients' AS model_name,
    'identifier' AS label_type

FROM {{ ref('op_retropgf_optimism_recipients') }}

UNION ALL

SELECT
    blockchain,
    voter AS address,
    'OP RetroPGF ' || round_name || ' Voter' AS name,
    'op_retropgf' AS category,
    'msilb7' AS contributor,
    'query' AS source,
    date('2023-03-30') AS created_at,
    now() AS updated_at,
    'op_retropgf_voters' AS model_name,
    'identifier' AS label_type

FROM {{ ref('op_retropgf_optimism_voters') }}
