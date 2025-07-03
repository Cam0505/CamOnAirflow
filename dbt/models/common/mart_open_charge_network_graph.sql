WITH driving_distances AS (
    SELECT
        id_1
        , id_2
        , name_1 AS source_name
        , name_2 AS target_name
        , driving_distance_km
    FROM {{ ref('base_open_charge_driving_distance') }}
    WHERE driving_distance_km IS NOT NULL
)

, source_stations AS (
    SELECT
        d.id_1
        , d.source_name
        , s.region AS source_region
        , s.town AS source_town
        , s.operator AS source_operator
        , s.usage_type AS source_usage_type
        , s.number_of_points AS source_points
        , {{ get_node_color('s.usage_type', 's.number_of_points') }} AS source_color
    FROM driving_distances AS d
    LEFT JOIN {{ source('opencharge', 'opencharge_stations') }} AS s
        ON d.id_1 = s.id
)

, target_stations AS (
    SELECT
        d.id_2
        , d.target_name
        , s.region AS target_region
        , s.town AS target_town
        , s.operator AS target_operator
        , s.usage_type AS target_usage_type
        , s.number_of_points AS target_points
        , {{ get_node_color('s.usage_type', 's.number_of_points') }} AS target_color
    FROM driving_distances AS d
    LEFT JOIN {{ source('opencharge', 'opencharge_stations') }} AS s
        ON d.id_2 = s.id
)

-- Final format optimized for graph visualization
SELECT
    d.id_1
    , s.source_name
    , s.source_region AS region
    , s.source_town AS town
    , s.source_operator AS operator
    , s.source_usage_type AS usage_type
    , s.source_points AS number_of_points
    , s.source_color AS node_color
    , d.id_2 AS target_id
    , t.target_name AS target_node
    , d.driving_distance_km
    , {{ get_edge_weight('d.driving_distance_km') }} AS connection_strength
FROM driving_distances AS d
INNER JOIN source_stations AS s ON d.id_1 = s.id_1
INNER JOIN target_stations AS t ON d.id_2 = t.id_2


-- ##
-- ## Configuration for Draw.io CSV Import
-- #
-- # label: %source_name%<br><i style="color:gray;">%usage_type%</i><br><small>Region: %region%<br>Points: %number_of_points%</small>
-- # style: shape=rectangle;html=1;whiteSpace=wrap;rounded=1;fillColor=%node_color%;strokeColor=#000000;strokeWidth=1;fontColor=#000000;
-- # connect: {"from": "id_1", "to": "target_id", "style": "endArrow=classic;html=1;strokeWidth=%connection_strength%;labelBackgroundColor=#ffffff;fontSize=10;", "label": "%driving_distance_km% km"}
-- # identity: id_1
-- # namespace: csvimport-
-- # layout: auto
-- # nodespacing: 40
-- # edgespacing: 50
-- # ignorecolumns: operator,town
-- #
-- ## ---- CSV starts below ----
-- id_1,source_name,region,town,operator,usage_type,number_of_points,node_color,target_id,target_node,driving_distance_km,connection_strength