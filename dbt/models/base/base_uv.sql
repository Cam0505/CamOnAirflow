-- ------------------------------------------------------------------------------
-- Model: Base_uv
-- Description: uv data from global api
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-05 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

SELECT date(uv_time) as uv_date, uv, uv_max, uv_time, ozone, city, location__lat, location__lng
-- FROM uv_data.uv_index
From {{ source("uv", "uv_index") }} 