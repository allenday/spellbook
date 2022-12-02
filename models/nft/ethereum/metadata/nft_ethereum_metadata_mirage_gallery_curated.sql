{{ config(
        alias = 'mirage_gallery_curated'
        )
}}

SELECT
    contract_address
    , project_id
    , project_id_base_value
    , collection_name
    , artist_name
    , mirage_project_name
    , art_collection_unique_id
FROM (VALUES
        ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 1, 10000, 'Ebbs AND Flows: Our Universe', 'Roope Rainisto & SOMNAI', 'Otherwhere', '0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5-1')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 2, 10000, 'Ebbs AND Flows: Our Universe', 'Roope Rainisto & SOMNAI', 'ANIMA', '0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5-2')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 3, 10000, 'Life AND Death: An Exploration of Impermanence', 'Austiin', 'Remnants', '0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5-3')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 4, 10000, 'Then AND Now: Ever-Changing Worlds', 'Revrart', 'Voyage', '0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5-4')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 5, 10000, 'Incoherent Elegance', 'Saucebook', 'Embracing Chaos', '0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5-5')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 6, 10000, 'Latent Travels', 'Rikkar', 'Yūgen', '0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5-6')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 7, 10000, 'New Dimension', 'Huemin', 'Seek', '0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5-7')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 8, 10000, 'Entangled Structures', 'Inner_Sanctum & Pancakes', 'MOODs', '0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5-8')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 9, 10000, 'Artifical Pathways', 'H01 & DeltaSauce', 'Nexus', '0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5-9')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 10, 10000, 'AI Art is Not Art', 'Claire Silver', 'Page', '0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5-10')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 11, 10000, 'Abstract (ART)chitecture', 'MrHabMo', 'Esquisse', '0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5-11')

)

ORDER BY project_id ASC
