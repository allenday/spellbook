{{config(alias='token_standards')}}


{% set labels_models = [
ref('labels_token_standards_arbitrum')
 ,ref('labels_token_standards_avalanche_c')
 ,ref('labels_token_standards_bnb')
 ,ref('labels_token_standards_ethereum')
 ,ref('labels_token_standards_ethereum')
 ,ref('labels_token_standards_fantom')
 ,ref('labels_token_standards_gnosis')
 ,ref('labels_token_standards_goerli')
 ,ref('labels_token_standards_optimism')
 ,ref('labels_token_standards_polygon')
] %}


SELECT *
FROM (
        {% for label in labels_models %}
        SELECT *
        FROM  {{ label }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)