WITH airports_regions_join AS (
    SELECT * 
    FROM {{source('staging_flights', 'airports')}}
    LEFT JOIN {{source('staging_flights', 'regions')}}
    USING (country)
)
SELECT * FROM airports_regions_join