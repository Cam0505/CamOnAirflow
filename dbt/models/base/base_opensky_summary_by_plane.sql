with base as (
    select
        icao24,
        departure_airport,
        arrival_airport,
        callsign,
        departure_datetime,
        arrival_datetime,
        distance_km,
        cast(departure_datetime as timestamp) as dep_ts,
        cast(arrival_datetime as timestamp) as arr_ts
    from {{ source('opensky', 'flight_tracking') }}
    where arrival_airport is not null
      and departure_airport is not null
      and distance_km is not null
      and departure_datetime is not null
      and arrival_datetime is not null
),

enriched as (
    select
        *,
        date_trunc('week', arr_ts) as week,
        date_trunc('month', arr_ts) as month,
        date_trunc('year', arr_ts) as year,
        extract(epoch from (arr_ts - dep_ts))/60 as flight_time_minutes
    from base
    where arr_ts > dep_ts
),

agg_by_period_plane as (
    select
        icao24,
        week,
        month,
        year,
        count(*) as total_flights,
        sum(distance_km) as total_distance_km,
        sum(flight_time_minutes) as total_flight_time_minutes,
        avg(distance_km) as avg_distance_km,
        avg(flight_time_minutes) as avg_flight_time_minutes
    from enriched
    group by icao24, week, month, year
),

popular_destinations as (
    select
        icao24,
        week,
        month,
        year,
        arrival_airport,
        count(*) as arrival_count,
        row_number() over (partition by icao24, week, month, year order by count(*) desc) as rn
    from enriched
    group by icao24, week, month, year, arrival_airport
),

popular_departures as (
    select
        icao24,
        week,
        month,
        year,
        departure_airport,
        count(*) as departure_count,
        row_number() over (partition by icao24, week, month, year order by count(*) desc) as rn
    from enriched
    group by icao24, week, month, year, departure_airport
)

select
    a.icao24,
    a.week,
    a.month,
    a.year,
    a.total_flights,
    a.total_distance_km,
    a.total_flight_time_minutes,
    a.avg_distance_km,
    a.avg_flight_time_minutes,
    pd.arrival_airport as most_popular_arrival_airport,
    pd.arrival_count as arrival_airport_flights,
    round(100.0 * pd.arrival_count / a.total_flights, 2) as pct_flights_arrival_airport,
    pdep.departure_airport as most_popular_departure_airport,
    pdep.departure_count as departure_airport_flights,
    round(100.0 * pdep.departure_count / a.total_flights, 2) as pct_flights_departure_airport
from agg_by_period_plane a
left join popular_destinations pd
    on a.icao24 = pd.icao24 and a.week = pd.week and a.month = pd.month and a.year = pd.year and pd.rn = 1
left join popular_departures pdep
    on a.icao24 = pdep.icao24 and a.week = pdep.week and a.month = pdep.month and a.year = pdep.year and pdep.rn = 1