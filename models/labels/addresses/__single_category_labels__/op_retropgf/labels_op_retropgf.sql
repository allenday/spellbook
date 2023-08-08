{{config(alias='op_retropgf')
}}

SELECT blockchain,
       submitter_address AS address, 
       'OP RetroPGF ' || round_name || ' Recipient - Submitter' as name,
       'op_retropgf' as category,
       'msilb7' as contributor,
       'query' AS source,
       date('2023-03-30') as created_at,
       CURRENT_TIMESTAMP() as updated_at,
        'op_retropgf_recipients' as model_name,
       'identifier' as label_type

FROM {{ ref('op_retropgf_optimism_recipients') }} 

UNION ALL

SELECT blockchain,
       voter AS address, 
       'OP RetroPGF ' || round_name || ' Voter' as name,
       'op_retropgf' as category,
       'msilb7' as contributor,
       'query' AS source,
       date('2023-03-30') as created_at,
       CURRENT_TIMESTAMP() as updated_at,
        'op_retropgf_voters' as model_name,
       'identifier' as label_type

FROM {{ ref('op_retropgf_optimism_voters') }}