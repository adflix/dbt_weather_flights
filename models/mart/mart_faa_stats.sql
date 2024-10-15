WITH departures AS (
    SELECT origin AS faa
           ,COUNT(origin) AS nunique_from 
           ,COUNT(sched_dep_time) AS dep_planned 
           ,SUM(cancelled) AS dep_cancelled 
           ,SUM(diverted) AS dep_diverted 
           ,COUNT(arr_time) AS dep_n_flights 
           ,COUNT(DISTINCT tail_number) AS dep_nunique_tails 
           ,COUNT(DISTINCT airline) AS dep_nunique_airlines 
    FROM {{ref('prep_flights')}}
    GROUP BY origin
),
arrivals AS (
    SELECT dest AS faa
           ,COUNT(dest) AS nunique_to 
           ,COUNT(sched_dep_time) AS arr_planned 
           ,SUM(cancelled) AS arr_cancelled 
           ,SUM(diverted) AS arr_diverted 
           ,COUNT(arr_time) AS arr_n_flights 
           ,COUNT(DISTINCT tail_number) AS arr_nunique_tails 
           ,COUNT(DISTINCT airline) AS arr_nunique_airlines 
    FROM {{ref('prep_flights')}}
    GROUP BY dest
),
Abflüge + Ankünfte
total_stats AS (
    SELECT COALESCE(departures.faa, arrivals.faa) AS faa
           ,COALESCE(nunique_to, 0) AS nunique_to
           ,COALESCE(nunique_from, 0) AS nunique_from
           ,COALESCE(dep_planned, 0) + COALESCE(arr_planned, 0) AS total_planned
           ,COALESCE(dep_cancelled, 0) + COALESCE(arr_cancelled, 0) AS total_canceled
           ,COALESCE(dep_diverted, 0) + COALESCE(arr_diverted, 0) AS total_diverted
           ,COALESCE(dep_n_flights, 0) + COALESCE(arr_n_flights, 0) AS total_flights
           ,COALESCE(dep_nunique_tails, 0) + COALESCE(arr_nunique_tails, 0) AS total_nunique_tails -- optionale Spalte für einzigartige Flugzeuge
           ,COALESCE(dep_nunique_airlines, 0) + COALESCE(arr_nunique_airlines, 0) AS total_nunique_airlines -- optionale Spalte für einzigartige Fluglinien
    FROM departures
    FULL JOIN arrivals ON departures.faa = arrivals.faa
)
-- Füge Stadt, Land und Namen des Flughafens hinzu
SELECT city  
       ,country
       ,name
       ,total_stats.*
FROM {{ref('prep_flights')}}
RIGHT JOIN total_stats ON {{ref('prep_flights')}}.origin = total_stats.faa OR {{ref('prep_flights')}}.dest = total_stats.faa
ORDER BY city