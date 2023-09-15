{{ config
(
    alias='first_funded_by'
    
)
}}

{% set addresses_events_models = [
    ref('addresses_events_ethereum_first_funded_by')
] %}

SELECT *
FROM (
    {% for addresses_events_model in addresses_events_models %}
    SELECT blockchain
    , address
    , first_funded_by
    , block_time
    , block_number
    , tx_hash
    FROM {{ addresses_events_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)