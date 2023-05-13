{{ config(alias='cex_users',
        post_hook='{{ expose_spells(\'["optimism","ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["msilb7"]\') }}'
) }}

{% set chains = [
    'optimism',
    'ethereum'
] %}

SELECT
    blockchain,
    address,
    name,
    category,
    contributor,
    source,
    created_at,
    updated_at,
    model_name,
    label_type
FROM (
    {% for chain in chains %}
        SELECT
            '{{ chain }}' AS blockchain,
            address,
            cex_name || ' User' AS name,
            'cex users' AS category,
            'msilb7' AS contributor,
            'query' AS source,
            timestamp('2023-03-11') AS created_at,
            now() AS updated_at,
            'cex_users_withdrawals' AS model_name,
            'persona' AS label_type

        FROM {{ source('erc20_' + chain, 'evt_transfer') }} AS t
        INNER JOIN {{ ref('addresses_'+ chain +'_cex') }} AS c
            ON t.`from` = c.address

        UNION ALL

        SELECT
            '{{ chain }}' AS blockchain,
            address,
            cex_name || ' User' AS name,
            'cex users' AS category,
            'msilb7' AS contributor,
            'query' AS source,
            timestamp('2023-03-11') AS created_at,
            now() AS updated_at,
            'cex_users_withdrawals' AS model_name,
            'persona' AS label_type


        FROM {{ source('erc20_' + chain, 'evt_transfer') }} AS t
        INNER JOIN {{ ref('addresses_'+ chain +'_cex') }} AS c
            ON t.`from` = c.address

        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
) AS a
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 --distinct if erc20 and eth
