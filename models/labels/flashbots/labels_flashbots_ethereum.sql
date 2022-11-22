{{config(alias='flashbots_ethereum',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}
{% if var('declare_values_with_unnest') %}
{% set array_start = '[' %}
{% set array_end = ']' %}
{% else %}
{% set array_start = 'ARRAY(' %}
{% set array_end = ')' %}
{% endif %}

SELECT DISTINCT {{ array_start }}'ethereum'{{ array_end }} AS blockchain
, account_address AS address
, 'Flashbots User' AS name
, 'flashbots' AS category
, 'hildobby' AS contributor
, 'query' AS source
, date('2022-10-08') AS created_at
, CURRENT_TIMESTAMP AS modified_at
FROM {{ source('flashbots','arbitrages') }}
