SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
--'Business report summarizing passenger and driver ratings by city'

CREATE OR REFRESH MATERIALIZED VIEW transportation.gold.business_rating_report_mv
COMMENT 'Business report summarizing passenger and driver ratings by city'
AS
SELECT
  city_name AS city,
  ROUND(AVG(passenger_rating), 2) AS avg_passenger_rating,
  ROUND(AVG(driver_rating), 2) AS avg_driver_rating
FROM transportation.gold.fact_trips_mv
GROUP BY city_name;