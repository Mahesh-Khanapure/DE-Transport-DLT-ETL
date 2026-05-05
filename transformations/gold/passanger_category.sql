
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.autoBroadcastJoinThreshold = 104857600; -- 100 MB

-- This materialized view summarizes trip data by state and passenger category.
CREATE OR REFRESH MATERIALIZED VIEW transportation.gold.passenger_category_mv
AS
SELECT
    c.state,
    t.passenger_category,
    COUNT(t.id) AS total_trips,
    SUM(t.sale_amt) AS revenue,
    ROUND(AVG(t.driver_rating), 2) AS avg_driver_rating
FROM transportation.silver.trips_scd2_with_current t
JOIN transportation.silver.city c
  ON t.city_id = c.city_id
WHERE t.__IS_CURRENT = true
GROUP BY c.state, t.city_id, t.passenger_category;