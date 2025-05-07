-- ------------------------------------------------------------------------------
-- Model: Base_Beverages
-- Description: Base Table for multiple Dims - Bev Type, Alcoholic Type and Beverage Name
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-03 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
SELECT bt.str_drink as Beverage_Name, 
    bt.id_drink as Beverage_ID, 
	bt.source_beverage_type as Beverage_Type,
	{{ dbt_utils.generate_surrogate_key(["source_beverage_type", "source_alcohol_type"]) }} as Beverage_Category_SK,
	act.source_alcohol_type as Alcoholic_type
	From {{ source("beverages", "beverages_table") }} as bt
    left join {{ source("beverages", "alcoholic_table") }} as act 
	on bt.id_drink = act.id_drink