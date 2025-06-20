-- ------------------------------------------------------------------------------
-- Model: Base_meal_ingredients
-- Description: Base Table for meal ingredients
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-07 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

SELECT id_ingredient as ingredient_id, str_ingredient as ingredient_name
FROM {{ source("meals", "ingredients") }} 