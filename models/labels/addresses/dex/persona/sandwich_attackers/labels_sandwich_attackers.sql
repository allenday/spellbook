{{
    config(
        alias='sandwich_attackers'
    )
}}

SELECT * FROM {{ ref('labels_sandwich_attackers_ethereum') }}