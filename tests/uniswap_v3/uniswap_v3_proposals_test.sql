-- Values were also manually checked on the Tally website https://www.tally.xyz/

WITH unit_tests AS (
    SELECT COALESCE(test_data.blockchain = un_proposals.blockchain
        AND test_data.proposal_id = un_proposals.proposal_id
        AND test_data.outcome = un_proposals.status, FALSE) AS test
    FROM {{ ref('uniswap_v3_ethereum_proposals') }} AS un_proposals
    INNER JOIN
        {{ ref('uniswap_v3_proposals_test') }} AS test_data ON
            test_data.proposal_id = un_proposals.proposal_id
)

SELECT
    COUNT(CASE WHEN test = FALSE THEN 1 END) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
HAVING COUNT(CASE WHEN test = FALSE THEN 1 END) > COUNT(*) * 0.1
