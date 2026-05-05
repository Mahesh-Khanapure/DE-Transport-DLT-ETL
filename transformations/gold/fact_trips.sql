
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.autoBroadcastJoinThreshold = 104857600;--100 MB

CREATE OR REFRESH MATERIALIZED VIEW transportation.gold.fact_trips_mv
COMMENT "Fact table combining trips and city data"
AS
SELECT 
    t.id AS trip_id,
    t.business_date,
    t.city_id,
    c.city_name,
    c.state,
    t.passenger_category,
    t.distance_kms,
    t.sale_amt,
    t.passenger_rating,
    t.driver_rating,
    t.silver_processed_timestamp
FROM transportation.silver.trips t
LEFT JOIN transportation.silver.city c
    ON t.city_id = c.city_id;