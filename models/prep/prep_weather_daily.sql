WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
        , DATE_PART('day', date) AS date_day
        , DATE_PART('month', date) AS date_month
        , DATE_PART('year', date) AS date_year
        , DATE_PART('week', date) AS cw  -- Calendar week
        , TO_CHAR(date, 'Month') AS month_name
        , TO_CHAR(date, 'Day') AS weekday
    FROM daily_data 
),
add_more_features AS (
    SELECT *
        , (CASE 
            WHEN date_month IN (12, 1, 2) THEN 'winter'
            WHEN date_month IN (3, 4, 5) THEN 'spring'
            WHEN date_month IN (6, 7, 8) THEN 'summer'
            WHEN date_month IN (9, 10, 11) THEN 'autumn'
        END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date;
