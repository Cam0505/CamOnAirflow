
/*
    dbt run --select +staging_crash_data
    Created: 03/11/2023 : Cam Martin - Initial Creation

*/


SELECT x, y, objectid, advisoryspeedtext, areaunitid, bicycle as bicycle_count, bridge as bridge_count, 
bus as bus_count, carstationwagon as car_stationwagon_count, cliffbank as cliffbank_count, 
crashdirectiondescription as crash_direction, crashfinancialyear, crashroadsideroad, 
crashyear, directionroledescription, fatalcount, fence as fence_count, 
flathill as gradient_category, guardrail as guardrail_count, 
holiday, case when holiday is null then 0 else 1 end as is_Holiday,
houseorbuilding as building_count, kerb as kerb_count, meshblockid, minorinjurycount, 
moped as mopeds_involved, motorcycle as motorcycles_involved, numberoflanes, objectthrownordropped, parkedvehicle as parkedvehicle_count, 
pedestrian as pedestrian_count, postorpole as postorpole_count, 
case when roadcharacter = 'Nil' then 'Other' else roadcharacter end as roadcharacter, case when roadworks >= 1 then 1 else 0 end as roadworks_involved, 
case when schoolbus >= 1 then 1 else 0 end as schoolbus_involved, 
seriousinjurycount, speedlimit, 
case when strayanimal >= 1 then 1 else 0 end as strayanimal_involved, suv as suvs_involved, temporaryspeedlimit, sliporflood,
trafficisland, trafficsign, case when train then 1 else 0 end as train_involved, 
case when tree >= 1 then 1 else 0 end as tree_involved,
case when truck >= 1 then 1 else 0 end as truck_involved, 
urban, vanorutility as van_or_ute_count, 
waterriver as bodies_of_water_involved, 
sswl.dim_weather_sk,
sstl.dim_tla_sk, sscls.dim_lanes_surface_sk, sscs.dim_streetlight_sk,
sscs2.dim_crashseverity_sk 

-- FROM public.base_crash_data as bcd
from {{ref('base_crash_data')}} as bcd
-- left join public.staging_source_weather_lookup sswl 
left join {{ref('staging_source_weather_lookup')}} as sswl
on bcd.weathera = sswl.weathera and bcd.weatherb = sswl.weatherb
-- left join public.staging_source_tla_lookup sstl 
left join {{ref('staging_source_tla_lookup')}} as sstl
on bcd.tlaid = sstl.tlaid and bcd.region = sstl.region
-- left join public.staging_source_crash_lanes_surface sscls 
left join {{ref('staging_source_crash_lanes_surface')}} as sscls
on bcd.roadlane = sscls.roadlane and bcd.roadsurface = sscls.roadsurface
-- left join public.staging_source_crash_streetlights sscs 
left join {{ref('staging_source_crash_streetlights')}} as sscs
on bcd.light = sscs.light_conditions and bcd.streetlight = sscs.streetlight 
-- left join public.staging_source_crash_severity sscs2 
left join {{ref('staging_source_crash_severity')}} as sscs2
on bcd.crashseverity  = sscs2.crashseverity