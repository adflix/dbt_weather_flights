WITH departures AS (
    SELECT flight_date::DATE AS flight_date,
           origin AS faa,
           COUNT(origin) AS nunique_from,
           COUNT(sched_dep_time) AS dep_planned,
           SUM(cancelled) AS dep_cancelled,
           SUM(diverted) AS dep_diverted,
           COUNT(arr_time) AS dep_n_flights,
           COUNT(DISTINCT tail_number) AS dep_nunique_tails, -- optionale Spalte für einzigartige Flugzeuge
           COUNT(DISTINCT airline) AS dep_nunique_airlines -- optionale Spalte für einzigartige Fluglinien
    FROM {{ ref('prep_flights') }}
    GROUP BY flight_date, origin
),
arrivals AS (
    SELECT flight_date::DATE AS flight_date,
           dest AS faa,
           COUNT(dest) AS nunique_to,
           COUNT(sched_dep_time) AS arr_planned,
           SUM(cancelled) AS arr_cancelled,
           SUM(diverted) AS arr_diverted,
           COUNT(arr_time) AS arr_n_flights,
           COUNT(DISTINCT tail_number) AS arr_nunique_tails, -- optionale Spalte für einzigartige Flugzeuge
           COUNT(DISTINCT airline) AS arr_nunique_airlines -- optionale Spalte für einzigartige Fluglinien
    FROM {{ ref('prep_flights') }}
    GROUP BY flight_date, dest
),
total_stats AS (
    SELECT COALESCE(departures.faa, arrivals.faa) AS faa,
           COALESCE(departures.flight_date, arrivals.flight_date) AS flight_date,
           COALESCE(nunique_to, 0) AS nunique_to,
           COALESCE(nunique_from, 0) AS nunique_from,
           COALESCE(dep_planned, 0) + COALESCE(arr_planned, 0) AS total_planned,
           COALESCE(dep_cancelled, 0) + COALESCE(arr_cancelled, 0) AS total_canceled,
           COALESCE(dep_diverted, 0) + COALESCE(arr_diverted, 0) AS total_diverted,
           COALESCE(dep_n_flights, 0) + COALESCE(arr_n_flights, 0) AS total_flights,
           COALESCE(dep_nunique_tails, 0) + COALESCE(arr_nunique_tails, 0) AS total_nunique_tails, -- optionale Spalte für einzigartige Flugzeuge
           COALESCE(dep_nunique_airlines, 0) + COALESCE(arr_nunique_airlines, 0) AS total_nunique_airlines -- optionale Spalte für einzigartige Fluglinien
    FROM departures
    FULL JOIN arrivals
    ON departures.faa = arrivals.faa
    AND departures.flight_date = arrivals.flight_date
),
weather_data AS (
    SELECT station AS faa,
           date AS weather_date,
           MIN(temperature_min) AS daily_min_temp,
           MAX(temperature_max) AS daily_max_temp,
           SUM(precipitation) AS daily_precipitation,
           SUM(snowfall) AS daily_snowfall,
           AVG(wind_direction) AS daily_avg_wind_direction,
           AVG(wind_speed) AS daily_avg_wind_speed,
           MAX(wind_peakgust) AS daily_wind_peakgust
    FROM {{ ref('weather_daily_raw') }}
    GROUP BY station, date
)
-- Endgültige Abfrage, die sowohl Flug- als auch Wetterdaten verbindet
SELECT airports.city,
       airports.country,
       airports.name,
       total_stats.*,
       weather_data.daily_min_temp,
       weather_data.daily_max_temp,
       weather_data.daily_precipitation,
       weather_data.daily_snowfall,
       weather_data.daily_avg_wind_direction,
       weather_data.daily_avg_wind_speed,
       weather_data.daily_wind_peakgust
FROM total_stats
LEFT JOIN weather_data
ON total_stats.faa = weather_data.faa
AND total_stats.flight_date = weather_data.weather_date
LEFT JOIN {{ ref('prep_airports') }} airports
ON total_stats.faa = airports.faa
ORDER BY total_stats.flight_date, airports.city