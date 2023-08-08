{{config(alias='project_wallets'
)}}

SELECT

    'optimism' as blockchain,
    address,
    project_name AS name,
    'project wallet' AS category,
    'msilb7' AS contributor,
    'static' AS source,
    timestamp('2023-01-28') as created_at,
    CURRENT_TIMESTAMP() AS updated_at,
    'project_wallets' AS model_name,
    'identifier' AS label_type

FROM {{ ref('addresses_optimism_grants_funding') }}
GROUP BY 1,2,3

-- UNION ALL

-- Add additional project wallet labels here