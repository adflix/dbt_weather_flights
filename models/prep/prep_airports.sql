WITH airports_reorder AS (
    SELECT faa
           ,name
           ,city
           ,region
           ,country
           ,lat
           ,lon
           ,tz
           ,dst
           ,alt
    FROM {{ref('staging_airports')}}
)
SELECT * 
FROM airports_reorder
