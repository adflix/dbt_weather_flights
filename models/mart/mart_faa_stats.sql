-- Entferne das "SELECT *" über dem WITH
WITH departures AS (
    SELECT origin AS faa
           ,COUNT(origin) AS nunique_from 
           ,COUNT(sched_dep_time) AS dep_planned -- geplante Abflüge
           ,SUM(cancelled) AS dep_cancelled -- stornierte Abflüge
           ,SUM(diverted) AS dep_diverted -- umgeleitete Abflüge
           ,COUNT(arr_time) AS dep_n_flights -- tatsächliche Abflüge
           ,COUNT(DISTINCT tail_number) AS dep_nunique_tails -- einzigartige Flugzeuge
           ,COUNT(DISTINCT airline) AS dep_nunique_airlines -- einzigartige Fluglinien
    FROM {{ref('prep_flights')}}
    GROUP BY origin
),
arrivals AS (
    SELECT dest AS faa
           ,COUNT(dest) AS nunique_to 
           ,COUNT(sched_dep_time) AS arr_planned -- geplante Ankünfte
           ,SUM(cancelled) AS arr_cancelled -- stornierte Ankünfte
           ,SUM(diverted) AS arr_diverted -- umgeleitete Ankünfte
           ,COUNT(arr_time) AS arr_n_flights -- tatsächliche Ankünfte
           ,COUNT(DISTINCT tail_number) AS arr_nunique_tails -- einzigartige Flugzeuge
           ,COUNT(DISTINCT airline) AS arr_nunique_airlines -- einzigartige Fluglinien
    FROM {{ref('prep_flights')}}
    GROUP BY dest
),
total_stats AS (
    SELECT faa
           ,nunique_to
           ,nunique_from
           ,dep_planned + arr_planned AS total_planed
           ,dep_cancelled + arr_cancelled AS total_canceled
           ,dep_diverted + arr_diverted AS total_diverted
           ,dep_n_flights + arr_n_flights AS total_flights
    FROM departures
    JOIN arrivals USING (faa)
)
-- Füge Stadt, Land und den Flughafennamen hinzu
SELECT city  
       ,country
       ,name
       ,total_stats.*
FROM {{ref('prep_flights')}}
RIGHT JOIN total_stats USING (faa)
ORDER BY city;