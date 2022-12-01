WITH unit_test AS (
    SELECT
        CASE WHEN test.action = actual.action THEN true ELSE false END AS action_test,
        CASE WHEN test.user = actual.user THEN true ELSE false END AS user_test,
        CASE WHEN test.jfiat_token_symbol = actual.jfiat_token_symbol THEN true ELSE false END AS jfiat_token_test,
        CASE WHEN ABS(test.jfiat_token_amount - actual.jfiat_token_amount) < 0.001 THEN true ELSE false END AS jfiat_token_amount_test,
        CASE WHEN test.collateral_symbol = actual.collateral_symbol THEN true ELSE false END AS collateral_token_test,
        CASE WHEN ABS(test.collateral_token_amount - actual.collateral_token_amount) < 0.001 THEN true ELSE false END AS collateral_token_amount_test,
        CASE WHEN ABS(test.net_collateral_amount - actual.net_collateral_amount) < 0.001 THEN true ELSE false END AS net_collateral_amount_test
    FROM {{ ref ('jarvis_network_polygon_all_transactions') }} AS actual
    INNER JOIN {{ ref ('jarvis_network_polygon_view_transactions_seed') }} AS test
        ON (actual.evt_tx_hash = test.evt_tx_hash AND actual.evt_index = test.evt_index)
)

SELECT * FROM unit_test
WHERE (action_test = false
       OR user_test = false
       OR jfiat_token_test = false
       OR jfiat_token_amount_test = false
       OR collateral_token_test = false
       OR collateral_token_amount_test = false
       OR net_collateral_amount_test = false)
