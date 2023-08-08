{{ config(
        alias = 'chromie_squiggle'
        )
}}

SELECT token_id, color_direction, color_spread, end_color, height, segments, spectrum, start_color, steps_between, squiggle_type, day_zero, harmonic
FROM
  {{ ref( 'nft_ethereum_metadata_chromie_squiggle_0_seed' ) }}