{{ config(
        alias='erc20_noncompliant',
        materialized ='table',
        file_format = 'delta'
) 
}}

SELECT distinct token_address
FROM {{ ref('transfers_ethereum_erc20_rolling_day') }}
where round(amount / power(10, 18), 6) < -0.001