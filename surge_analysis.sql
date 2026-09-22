-- Surge Demand Analysis
-- Surge threshold based on 90th percentile of test demand

CREATE OR REPLACE TABLE workspace.default.surge_demand AS
WITH threshold AS (
    SELECT percentile_approx(demand, 0.90) AS surge_threshold
    FROM workspace.default.xgb_predictions
)
SELECT
    p.*,
    t.surge_threshold,
    CASE WHEN p.demand >= t.surge_threshold THEN 1 ELSE 0 END AS surge_flag
FROM workspace.default.xgb_predictions p
CROSS JOIN threshold t;

-- Top Surge Pickup Locations
SELECT
    PULocationID,
    COUNT(*) AS surge_hours,
    ROUND(AVG(demand),2) AS avg_surge_demand,
    MAX(demand) AS peak_demand
FROM workspace.default.surge_demand
WHERE surge_flag = 1
GROUP BY PULocationID
ORDER BY surge_hours DESC
LIMIT 20;

-- Surge by Hour
SELECT
    HOUR(demand_hour) AS hour,
    COUNT(*) AS surge_hours,
    ROUND(AVG(demand),2) AS avg_surge_demand,
    MAX(demand) AS peak_demand
FROM workspace.default.surge_demand
WHERE surge_flag = 1
GROUP BY HOUR(demand_hour)
ORDER BY surge_hours DESC;

-- Dashboard Summary
CREATE OR REPLACE TABLE workspace.default.dashboard_summary AS
SELECT
    HOUR(demand_hour) AS hour,
    PULocationID,
    SUM(demand) AS total_demand,
    AVG(prediction) AS avg_predicted_demand,
    MAX(demand) AS peak_demand,
    SUM(surge_flag) AS surge_hours
FROM workspace.default.surge_demand
GROUP BY HOUR(demand_hour), PULocationID;
