WITH trades AS (
    SELECT
        block_time,
        token_bought_amount,
        token_sold_amount,
        tx_hash,
        evt_index
    FROM {{ ref('platypus_finance_avalanche_c_trades') }}
    WHERE 1 = 0
        -- 20 manually tested swaps
        OR (
            tx_hash = '0xc9cf002b6422ef0a617284537683372c66b92e84e0b28fde8a0cc04a4eef519e' AND evt_index = '23'
        )
        OR (
            tx_hash = '0x75dc4b71defb76d9888aabbd8771e8b38ee48fb41d43cc3ecae6fb73b3911c84' AND evt_index = '20'
        )
        OR (
            tx_hash = '0x93bd89cf8a4d602d5cbc32446e5fb4bf9ed170f5ac72c2dc23294c8f5e1a8a05' AND evt_index = '26'
        )
        OR (
            tx_hash = '0x306818d93ecd131c5e5e40a2293150db9484555d539a45e0512cc28a7041ebfb' AND evt_index = '45'
        )
        OR (
            tx_hash = '0x1bbe2f7773c059f500de2cd6acdee778d04b8c8185d4c69bb20db835feda9b76' AND evt_index = '24'
        )
        OR (
            tx_hash = '0xd1d05ff16c664884875cf17ded334008fe1005b15103460c76a8979a791d3cc1' AND evt_index = '13'
        )
        OR (
            tx_hash = '0x7fa1daa95f0c034752cb3719b62e7ae3d372945db163bda78e26cdc3a18192c3' AND evt_index = '6'
        )
        OR (
            tx_hash = '0xd1e3319eb5ab9cae929d2a18e79facb10309f84a8e38073596df084745c2f2f1' AND evt_index = '113'
        )
        OR (
            tx_hash = '0x916f2f560b3c13f31aa4139608b067e00ad042bd3bdb197a683e68d77c80aab0' AND evt_index = '12'
        )
        OR (
            tx_hash = '0x9ac609a5cf6084d4152e835fcf790f45dbdb363bda79afc62ac3dfc5235eea7d' AND evt_index = '17'
        )
        OR (
            tx_hash = '0x2a3a710fa23fe85c0153a701167ac8265327e08c4e527e9de3d0727099934f48' AND evt_index = '8'
        )
        OR (
            tx_hash = '0xa268e678b9167e2afbff624d6c473e1de4e69e00bbbf15cff30fec754965161e' AND evt_index = '80'
        )
        OR (
            tx_hash = '0x4587aca823136fd6dbe9ecf9a992074062c49d13ee38fc65a7f547e5647c40d6' AND evt_index = '34'
        )
        OR (
            tx_hash = '0x2747e96f2e6198cbaad9a34257faae3d1282401ec89c4d8e2b8980830417f7e4' AND evt_index = '26'
        )
        OR (
            tx_hash = '0x2954327b19870057c067530f4f951014163795afe991f5cd2657649ab8151a88' AND evt_index = '74'
        )
        OR (
            tx_hash = '0x48596630bee61338ac47f100539d58fd783215ceb624d888249818b9c7eade5f' AND evt_index = '59'
        )
        OR (
            tx_hash = '0x138e58cdee3bd8ccb09bfaaffe340f84c29688d4a239dab4b94d12e49434d5a9' AND evt_index = '82'
        )
        OR (
            tx_hash = '0x363e3d084f738cb84cc809ca6ec9738bc23ceb606ca13148fe210b0bb098115b' AND evt_index = '22'
        )
        OR (
            tx_hash = '0x70aa1e9d8f0698b0b4b118b5296d67a3fb15f380a8ca105ba3c433655ae93aa6' AND evt_index = '28'
        )
        OR (
            tx_hash = '0x9e982c5f221d878d5c30291f3b1af3bb4896a0d15ecc305ff4d4e63936ed191e' AND evt_index = '64'
        )
),

examples AS (
    SELECT * FROM {{ ref('dex_trades_seed') }}
    WHERE
        blockchain = 'avalanche_c' AND project = 'platypus_finance' AND version = '1'
),

matched AS (
    SELECT
        block_date,
        trades.tx_hash AS tr_tx_hash,
        examples.tx_hash AS ex_tx_hash,
        '|' AS `|1|`,
        trades.evt_index AS tr_evt_index,
        examples.evt_index AS ex_evt_index,
        '|' AS `|2|`,
        trades.token_bought_amount AS tr_token_bought_amount,
        examples.token_bought_amount AS ex_token_bought_amount,
        '|' AS `|3|`,
        trades.token_sold_amount AS tr_token_sold_amount,
        examples.token_sold_amount AS ex_token_sold_amount,
        COALESCE((
            trades.token_bought_amount - examples.token_bought_amount
        ) < 0.01,
        FALSE) AS correct_bought_amount,
        trades.token_bought_amount - examples.token_bought_amount AS `Δ token_bought_amount`,
        COALESCE((trades.token_sold_amount - examples.token_sold_amount) < 0.01,
            FALSE) AS correct_sold_amount,
        trades.token_sold_amount - examples.token_sold_amount AS `Δ token_sold_amount`
    FROM trades
    FULL OUTER JOIN examples
        ON
            trades.tx_hash = examples.tx_hash AND trades.evt_index = examples.evt_index
)

SELECT * FROM matched
WHERE NOT(correct_bought_amount AND correct_sold_amount)
