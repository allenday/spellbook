{{ config(
        alias = 'terraforms'
        )
}}

SELECT token_id, mode, chroma, biome, terraform_zone, x_coordinate, y_coordinate, terraform_level, question_marks, x_seed, y_seed, lith0, spine
FROM
  {{ ref( 'nft_ethereum_metadata_terraforms_0_seed' ) }}