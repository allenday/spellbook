{{ config(
        alias ='aggregators')
}}


SELECT 'ethereum' as blockchain, * FROM  {{ ref('nft_ethereum_aggregators') }}
