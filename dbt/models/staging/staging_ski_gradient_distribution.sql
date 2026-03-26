WITH segments AS (
    SELECT
        resort,
        country_code,
        length_m,
        ABS(ATAN(gradient / 100.0) * 180.0 / PI()) AS gradient_deg
    FROM {{ ref('base_filtered_ski_segments') }}
),

binned AS (
    SELECT
        resort,
        country_code,
        CAST(FLOOR((gradient_deg - 5) / 2.25) AS INTEGER) AS gradient_bin, -- 20 bins between 5 and 50 degrees
        SUM(length_m) AS terrain_m
    FROM segments
    WHERE gradient_deg IS NOT NULL
      AND gradient_deg >= 5
      AND gradient_deg <= 50
      AND length_m IS NOT NULL
      AND length_m > 0
    GROUP BY resort, country_code, gradient_bin
)

SELECT
    resort,
    country_code,
    gradient_bin,
    terrain_m,
    5 + gradient_bin * 2.25 AS gradient_bin_center_deg
FROM binned