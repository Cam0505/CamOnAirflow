-- ------------------------------------------------------------------------------
-- Model: Staging_Beverage_Consumption
-- Description: Fact Table data, consumption events generated from API 
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-03 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

SELECT id_drink as Beverage_Id, str_drink as Beverage_Name, 
bcl.Beverage_Category_SK,
str_glass as Glass_Type,
bcl.Beverage_Type_SK,
bcl.Beverage_Type,
bcl.Alcoholic_Type_SK,
bcl.Alcoholic_type,
str_category, 
str_alcoholic, 
bgl.glass_type_sk,
str_instructions as Beverage_Instructions, str_drink_thumb as beverage_url, date_melbourne,
str_ingredient1, str_ingredient2, 
str_ingredient3, str_ingredient4, str_ingredient5, 
str_ingredient6, str_ingredient7, str_ingredient8
	-- FROM public_base.base_beverage_consumption as bc
    FROM {{ref('base_beverage_consumption')}} as bc
	-- left join public_base.base_beverage_glass_lookup as bgl
    left join {{ref('base_beverage_glass_lookup')}}  as bgl
	on bc.str_glass = bgl.glass_type
	left join {{ref('staging_beverage_lookup')}}  as bcl
	on bc.id_drink = bcl.Beverage_ID
	