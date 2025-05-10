SELECT Beverage_Name, 
	-- Used in Dim_Beverage to connect to Consumption
    Beverage_ID, 
	Beverage_Type,
	-- Used in Dim_Beverage and Dim_Beverage_Type as the connection
	{{ dbt_utils.generate_surrogate_key(["Beverage_Type"]) }} as Beverage_Type_SK,
	-- Used in Dim_Alcoholic_Type and Dim_Beverage_Type as connection
	{{ dbt_utils.generate_surrogate_key(["Beverage_Type", "Alcoholic_type"]) }} as Beverage_Category_SK,
	Alcoholic_type,
	-- In Dim_Alcoholic_Type encase any future fact tables need to connect directly 
	{{ dbt_utils.generate_surrogate_key(["Alcoholic_type"]) }} as Alcoholic_Type_SK
    From {{ref('base_beverages')}} 