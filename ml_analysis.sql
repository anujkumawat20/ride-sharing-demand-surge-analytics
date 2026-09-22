-- Machine Learning Analysis
-- XGBoost prediction and model evaluation

-- Actual vs Predicted by Pickup Location
SELECT
    PULocationID,
    ROUND(AVG(demand), 2) AS avg_actual_demand,
    ROUND(AVG(prediction), 2) AS avg_predicted_demand,
    ROUND(AVG(ABS(demand - prediction)), 2) AS avg_error
FROM workspace.default.xgb_predictions
GROUP BY PULocationID
ORDER BY avg_actual_demand DESC
LIMIT 20;

-- Actual vs Predicted Demand by Hour
SELECT
    HOUR(demand_hour) AS hour,
    ROUND(AVG(demand), 2) AS avg_actual_demand,
    ROUND(AVG(prediction), 2) AS avg_predicted_demand
FROM workspace.default.xgb_predictions
GROUP BY HOUR(demand_hour)
ORDER BY hour;

-- Model Metrics
SELECT * FROM workspace.default.ml_metrics;
