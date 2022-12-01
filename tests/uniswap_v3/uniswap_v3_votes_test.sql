-- Values were also manually checked on the Tally website https://www.tally.xyz/

WITH unit_tests AS (SELECT COALESCE(test_data.blockchain = un_votes.blockchain
    AND test_data.proposal_id = un_votes.proposal_id
    AND test_data.voter_address = un_votes.voter_address
    AND ROUND(
        test_data.votes / 1e6, 1
    ) = ROUND(un_votes.votes / 1e6, 1), FALSE) AS test
    FROM {{ ref('uniswap_v3_ethereum_votes') }} AS un_votes
    INNER JOIN
        {{ ref('uniswap_v3_votes_test') }} AS test_data ON
            test_data.proposal_id = un_votes.proposal_id
            AND test_data.voter_address = un_votes.voter_address
)

SELECT
    COUNT(CASE WHEN test = FALSE THEN 1 END) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
HAVING COUNT(CASE WHEN test = FALSE THEN 1 END) > COUNT(*) * 0.1
