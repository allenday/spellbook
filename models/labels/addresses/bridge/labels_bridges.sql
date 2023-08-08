{{ config(
    alias = 'bridge',
    materialized = 'view')
}}

{% set bridges_models = [
 ref('labels_bridges_ethereum')
 , ref('labels_bridges_fantom')
] %}

SELECT *
FROM (
    {% for bridges_model in bridges_models %}
    SELECT
        blockchain
        , address
        , name
        , category
        , contributor
        , source
        , created_at
        , updated_at
        , model_name
        , label_type
    FROM {{ bridges_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)