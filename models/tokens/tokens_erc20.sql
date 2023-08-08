{{ config( alias='erc20',
        materialized = 'view',
        tags=['static'])}}


SELECT 'ethereum' as blockchain, * FROM  {{ ref('tokens_ethereum_erc20') }}
