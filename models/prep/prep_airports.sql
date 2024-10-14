WITH airports_reorder AS (
    SELECT faa
           ,name
           ,city
           ,state
           ,country
           ,lat
           ,lon
           ,tz
    FROM {{ref('staging_airports')}}
)
SELECT * 
FROM airports_reorder
