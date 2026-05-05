SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
--this view summarizes the statewise sale amount
CREATE OR REFRESH MATERIALIZED VIEW transportation.gold.statewise_sale_amount
AS
SELECT
  state,
  SUM(sale_amt) AS total_sale_amount
FROM transportation.gold.fact_trips_mv
GROUP BY state;