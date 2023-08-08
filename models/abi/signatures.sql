{{ config(
        alias = 'signatures',
        schema = 'abi',
        partition_by = {"field": "created_at_month"},
        materialized = 'view',
                        unique_key = ['created_at', 'unique_signature_id']
        )
}}

{% set chains = [
    source('ethereum', 'signatures')
    ,source('optimism', 'signatures')
    ,source('arbitrum', 'signatures')
    ,source('avalanche_c', 'signatures')
    ,source('polygon', 'signatures')
    ,source('bnb', 'signatures')
    ,source('gnosis', 'signatures')
    ,source('fantom', 'signatures')
] %}

WITH
    signatures as (
        {% for chain_source in chains %}

            SELECT
                abi,
                created_at,
                id,
                signature,
                type,
                concat(id, signature, type) as unique_signature_id
            FROM {{ chain_source }}

            {% if is_incremental() %}
            WHERE created_at >= date_trunc("day", CURRENT_TIMESTAMP() - interval '2 days')
            {% endif %}

            {% if not loop.last %}
            union all
            {% endif %}

        {% endfor %}
    )

    SELECT
    *
    FROM (
        SELECT
            id
            , signature
            , abi
            , type
            , created_at
            , TIMESTAMP_TRUNC(created_at, month) as created_at_month
            , unique_signature_id
            , row_number() over (partition by unique_signature_id order by created_at desc) recency
        FROM signatures
    ) a
    WHERE recency = 1
    {% if is_incremental() %}
    AND NOT EXISTS (SELECT 1 FROM {{ this }} WHERE unique_signature_id = a.unique_signature_id)
    {% endif %}