{{ config(
        alias = 'cryptodickbutts'
        )
}}

SELECT token_id,background,body,butt,dick,eyes,hand,hat,legendary,mouth,nose,shoes,skin,special,trait_count
FROM
  {{ ref( 'nft_ethereum_metadata_cryptodickbutts_0_seed' ) }}