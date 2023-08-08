{{
    config(
        alias='contract_deployers_fantom'
    )
}}

SELECT distinct 'fantom'             AS blockchain
              , creation.`from`      AS address
              , 'Contract Deployer'  AS name
              , 'infrastructure'     AS category
              , 'hildobby'           AS contributor
              , 'query'              AS source
              , date('2023-03-03')   AS created_at
              , CURRENT_TIMESTAMP()                AS updated_at
              , 'contract_deployers' AS model_name
              , 'persona'            AS label_type
FROM {{ source('fantom', 'creation_traces') }} creation
LEFT ANTI JOIN {{ source('fantom', 'creation_traces') }} anti_table 
ON creation.from = anti_table.address