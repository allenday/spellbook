 {{
  config(
    alias='nft_bridged_mapping',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "tokens",
                                    \'["chuxin"]\') }}'
  )
}}
SELECT
  n.category AS category
  ,b.`remoteToken` AS contract_address
  ,n.name
  ,n.standard
  ,n.symbol
  ,b.`localToken` AS contract_address_l1
from {{ source('optimism_ethereum','L1ERC721Bridge_evt_ERC721BridgeInitiated') }} AS b
LEFT JOIN {{ ref('tokens_ethereum_nft')}} AS n
  on n.contract_address = b.`localToken`
group by 1, 2, 3, 4, 5, 6
