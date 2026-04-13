WITH segments AS (
    SELECT
        resort,
        country_code,
        length_m,
        LEAST(
            55.0,
            GREATEST(
                ABS(ATAN(gradient / 100.0) * 180.0 / PI()),
                ABS(ATAN(original_gradient / 100.0) * 180.0 / PI())
            )
        ) AS gradient_deg
    FROM {{ ref('base_filtered_ski_segments') }}
),

binned AS (
    SELECT
        resort,
        country_code,
        CAST(FLOOR((gradient_deg - 5) / 2.5) AS INTEGER) AS gradient_bin, -- 21 bins between 5 and 55 degrees; values above 55 are capped into the top bin
        SUM(length_m) AS terrain_m
    FROM segments
    WHERE gradient_deg IS NOT NULL
      AND gradient_deg >= 5
      AND gradient_deg <= 55
      AND length_m IS NOT NULL
      AND length_m > 0
    GROUP BY resort, country_code, gradient_bin
)

SELECT
    resort,
    country_code,
    gradient_bin,
    terrain_m,
    5 + gradient_bin * 2.5 AS gradient_bin_center_deg
FROM binned