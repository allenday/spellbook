{{ config(
    alias = 'all',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                "sector",
                                "labels",
                                \'["soispoke","hildobby"]\') }}')
}}

-- Static Labels
SELECT * FROM {{ ref('labels_cex') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_funds') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_bridges') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_ofac_sanctionned_ethereum') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_multisig_ethereum') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_hackers_ethereum') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_mev_ethereum') }}
UNION DISTINCT
SELECT blockchain, address, name, category, contributor, source, created_at, updated_at FROM {{ ref('labels_aztec_v2_contracts_ethereum') }}
UNION DISTINCT
-- Query Labels
SELECT * FROM {{ ref('labels_nft') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_safe_ethereum') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_tornado_cash') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_contracts') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_miners') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_airdrop_1_receivers_optimism') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_arbitrage_traders')}}
UNION DISTINCT
SELECT * FROM {{ ref('labels_flashbots_ethereum') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_ens') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_validators') }}
UNION DISTINCT
SELECT * FROM {{ ref('labels_sandwich_attackers') }}
