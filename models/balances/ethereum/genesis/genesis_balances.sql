{{ config(
	alias ='genesis_balances'
	)
}}

SELECT 
	`address`,
	CAST(balance_raw AS NUMERIC) AS balance_raw,
	CAST(balance_raw AS NUMERIC) / power(10,18) as balance 
FROM 
	{{ ref ( 'genesis_balances_seed' )}}