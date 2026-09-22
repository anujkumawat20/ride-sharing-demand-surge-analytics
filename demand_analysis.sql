-- Ride-Sharing Demand Analysis
-- NYC TLC Yellow Taxi - January 2025

-- Demand by Hour
SELECT pickup_hour, COUNT(*) AS total_trips
FROM workspace.default.taxi_trips
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- Top Pickup Locations
SELECT PULocationID, COUNT(*) AS total_trips
FROM workspace.default.taxi_trips
GROUP BY PULocationID
ORDER BY total_trips DESC
LIMIT 20;

-- Demand by Day of Week
SELECT pickup_day_of_week, COUNT(*) AS total_trips
FROM workspace.default.taxi_trips
GROUP BY pickup_day_of_week
ORDER BY pickup_day_of_week;

-- Hourly Demand Table
CREATE OR REPLACE TABLE workspace.default.hourly_demand AS
SELECT
    date_trunc('hour', tpep_pickup_datetime) AS demand_hour,
    PULocationID,
    COUNT(*) AS demand
FROM workspace.default.taxi_trips
GROUP BY
    date_trunc('hour', tpep_pickup_datetime),
    PULocationID;
