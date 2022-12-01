-- Check for negative balances
-- Some balances are very small negative numbers due to loss of precision from large ints

SELECT bal.amount
FROM {{ ref('balances_ethereum_erc20_day') }} AS bal
WHERE round(bal.amount / power(10, 18), 6) < 0
    -- limiting to a selection of tokens because we haven't filtered out all non-compliant tokens
    AND bal.symbol IN ('AAVE', 'DAI', 'UNI', 'LINK')
    AND bal.day > now() - INTERVAL 2 DAYS
