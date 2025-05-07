-- With raw_crash_data as (
    Select * 
    From {{ source("raw_csv", "crash_data") }} 
    
-- )
