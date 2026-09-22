# Databricks notebook source
df = spark.read.parquet(
    "/Workspace/Users/kumawatanuj20@gmail.com/yellow_tripdata_2025-01.parquet"
)

display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import col, sum

df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).display()

# COMMAND ----------

display(df.describe())

# COMMAND ----------

from pyspark.sql.functions import col

df_clean = df.filter(
    (col("trip_distance") > 0) &
    (col("total_amount") > 0) &
    (col("passenger_count") > 0)
)

display(df_clean.limit(10))

# COMMAND ----------

from pyspark.sql.functions import (
    to_date, hour, dayofweek, month
)

df_clean = (
    df_clean
    .withColumn("pickup_date", to_date("tpep_pickup_datetime"))
    .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
    .withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime"))
    .withColumn("pickup_month", month("tpep_pickup_datetime"))
)

display(df_clean.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.ride_sharing_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.default;

# COMMAND ----------

df_clean.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/ride_sharing_data/cleaned_taxi"
)

# COMMAND ----------

df_cleaned = spark.read.parquet(
    "/Volumes/workspace/default/ride_sharing_data/cleaned_taxi"
)

display(df_cleaned.limit(10))

# COMMAND ----------

df_cleaned.write.mode("overwrite").saveAsTable(
    "workspace.default.taxi_trips"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_hour,
# MAGIC     COUNT(*) AS total_trips
# MAGIC FROM workspace.default.taxi_trips
# MAGIC GROUP BY pickup_hour
# MAGIC ORDER BY pickup_hour;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     PULocationID,
# MAGIC     COUNT(*) AS total_trips
# MAGIC FROM workspace.default.taxi_trips
# MAGIC GROUP BY PULocationID
# MAGIC ORDER BY total_trips DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_day_of_week,
# MAGIC     COUNT(*) AS total_trips
# MAGIC FROM workspace.default.taxi_trips
# MAGIC GROUP BY pickup_day_of_week
# MAGIC ORDER BY pickup_day_of_week;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_hour,
# MAGIC     COUNT(*) AS total_trips
# MAGIC FROM workspace.default.taxi_trips
# MAGIC GROUP BY pickup_hour
# MAGIC ORDER BY total_trips DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_hour,
# MAGIC     PULocationID,
# MAGIC     COUNT(*) AS total_trips
# MAGIC FROM workspace.default.taxi_trips
# MAGIC GROUP BY pickup_hour, PULocationID
# MAGIC ORDER BY total_trips DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.default.hourly_demand AS
# MAGIC SELECT
# MAGIC     date_trunc('hour', tpep_pickup_datetime) AS demand_hour,
# MAGIC     PULocationID,
# MAGIC     COUNT(*) AS demand
# MAGIC FROM workspace.default.taxi_trips
# MAGIC GROUP BY
# MAGIC     date_trunc('hour', tpep_pickup_datetime),
# MAGIC     PULocationID;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.default.hourly_demand
# MAGIC ORDER BY demand_hour
# MAGIC LIMIT 20;

# COMMAND ----------

from pyspark.sql.functions import hour, dayofweek, to_date

hourly_df = spark.table("workspace.default.hourly_demand")

hourly_df = (
    hourly_df
    .withColumn("hour", hour("demand_hour"))
    .withColumn("day_of_week", dayofweek("demand_hour"))
    .withColumn("date", to_date("demand_hour"))
)

display(hourly_df.limit(10))

# COMMAND ----------

df_clean = df_clean.dropDuplicates()

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, col

df_clean = df_clean.filter(
    unix_timestamp("tpep_dropoff_datetime") >
    unix_timestamp("tpep_pickup_datetime")
)

# COMMAND ----------

df_clean = df_clean.filter(
    (col("trip_distance") > 0) &
    (col("trip_distance") <= 100)
)

# COMMAND ----------

df_clean = df_clean.filter(
    (col("passenger_count") >= 1) &
    (col("passenger_count") <= 6)
)

# COMMAND ----------

print("Rows after cleaning:", df_clean.count())

# COMMAND ----------

from pyspark.sql.functions import col, sum

display(df_clean.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_clean.columns
]))

# COMMAND ----------

clean_df = raw_df.filter(
    (col("trip_distance") > 0) &
    (col("trip_distance") <= 100) &
    (col("total_amount") > 0) &
    (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
)

# COMMAND ----------

print("Raw rows:", raw_df.count())
print("Clean rows:", clean_df.count())
print("Rows removed:", raw_df.count() - clean_df.count())

# COMMAND ----------

clean_df.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/ride_sharing_data/cleaned_taxi"
)


# COMMAND ----------

clean_df.write.mode("overwrite").saveAsTable(
    "workspace.default.taxi_trips"
)

# COMMAND ----------

from pyspark.sql.functions import to_date, hour, dayofweek

clean_df = (
    clean_df
    .withColumn("pickup_date", to_date("tpep_pickup_datetime"))
    .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
    .withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime"))
)

display(clean_df.limit(10))

# COMMAND ----------

clean_df.write.mode("overwrite").saveAsTable(
    "workspace.default.taxi_trips"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_hour,
# MAGIC     COUNT(*) AS total_trips
# MAGIC FROM workspace.default.taxi_trips
# MAGIC GROUP BY pickup_hour
# MAGIC ORDER BY pickup_hour;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     PULocationID,
# MAGIC     COUNT(*) AS total_trips
# MAGIC FROM workspace.default.taxi_trips
# MAGIC GROUP BY PULocationID
# MAGIC ORDER BY total_trips DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     PULocationID,
# MAGIC     COUNT(*) AS total_trips
# MAGIC FROM workspace.default.taxi_trips
# MAGIC GROUP BY PULocationID
# MAGIC ORDER BY total_trips DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_day_of_week,
# MAGIC     COUNT(*) AS total_trips
# MAGIC FROM workspace.default.taxi_trips
# MAGIC GROUP BY pickup_day_of_week
# MAGIC ORDER BY pickup_day_of_week;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.default.hourly_demand AS
# MAGIC SELECT
# MAGIC     date_trunc('hour', tpep_pickup_datetime) AS demand_hour,
# MAGIC     PULocationID,
# MAGIC     COUNT(*) AS demand
# MAGIC FROM workspace.default.taxi_trips
# MAGIC GROUP BY
# MAGIC     date_trunc('hour', tpep_pickup_datetime),
# MAGIC     PULocationID;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.default.hourly_demand
# MAGIC ORDER BY demand_hour
# MAGIC LIMIT 20;

# COMMAND ----------

from pyspark.sql.functions import hour, dayofweek, dayofmonth, month

hourly_df = spark.table("workspace.default.hourly_demand")

hourly_df = (
    hourly_df
    .withColumn("hour", hour("demand_hour"))
    .withColumn("day_of_week", dayofweek("demand_hour"))
    .withColumn("day_of_month", dayofmonth("demand_hour"))
    .withColumn("month", month("demand_hour"))
)

display(hourly_df.limit(10))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col

window = Window.partitionBy("PULocationID").orderBy("demand_hour")

hourly_df = (
    hourly_df
    .withColumn("demand_lag_1h", lag("demand", 1).over(window))
    .withColumn("demand_lag_2h", lag("demand", 2).over(window))
    .withColumn("demand_lag_24h", lag("demand", 24).over(window))
)

display(hourly_df.limit(10))

# COMMAND ----------

hourly_df = hourly_df.dropna(
    subset=[
        "demand_lag_1h",
        "demand_lag_2h",
        "demand_lag_24h"
    ]
)

print("ML rows:", hourly_df.count())


# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

feature_cols = [
    "PULocationID",
    "hour",
    "day_of_week",
    "day_of_month",
    "month",
    "demand_lag_1h",
    "demand_lag_2h",
    "demand_lag_24h"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

ml_df = assembler.transform(hourly_df).select(
    "demand_hour",
    "PULocationID",
    "demand",
    "features"
)

display(ml_df.limit(10))

# COMMAND ----------

train_df = ml_df.filter(
    col("demand_hour") < "2025-01-25"
)

test_df = ml_df.filter(
    col("demand_hour") >= "2025-01-25"
)

print("Training rows:", train_df.count())
print("Testing rows:", test_df.count())

# COMMAND ----------

train_df = ml_df.filter(
    col("demand_hour") < "2025-01-25"
)

test_df = ml_df.filter(
    col("demand_hour") >= "2025-01-25"
)

print("Training rows:", train_df.count())
print("Testing rows:", test_df.count())

# COMMAND ----------

from pyspark.ml.regression import RandomForestRegressor

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="demand",
    numTrees=100,
    maxDepth=10,
    seed=42
)

rf_model = rf.fit(train_df)

# COMMAND ----------

predictions = rf_model.transform(test_df)

display(
    predictions.select(
        "demand_hour",
        "PULocationID",
        "demand",
        "prediction"
    ).limit(20)
)

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

rmse = RegressionEvaluator(
    labelCol="demand",
    predictionCol="prediction",
    metricName="rmse"
).evaluate(predictions)

mae = RegressionEvaluator(
    labelCol="demand",
    predictionCol="prediction",
    metricName="mae"
).evaluate(predictions)

r2 = RegressionEvaluator(
    labelCol="demand",
    predictionCol="prediction",
    metricName="r2"
).evaluate(predictions)

print("RMSE:", rmse)
print("MAE:", mae)
print("R²:", r2)

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

rmse = RegressionEvaluator(
    labelCol="demand",
    predictionCol="prediction",
    metricName="rmse"
).evaluate(predictions)

mae = RegressionEvaluator(
    labelCol="demand",
    predictionCol="prediction",
    metricName="mae"
).evaluate(predictions)

r2 = RegressionEvaluator(
    labelCol="demand",
    predictionCol="prediction",
    metricName="r2"
).evaluate(predictions)

print("RMSE:", rmse)
print("MAE:", mae)
print("R²:", r2)

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %pip install xgboost

# COMMAND ----------

from xgboost.spark import SparkXGBRegressor

print("XGBoost is available")

# COMMAND ----------

from pyspark.sql.functions import hour, dayofweek, dayofmonth, month
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col
from pyspark.ml.feature import VectorAssembler

hourly_df = spark.table("workspace.default.hourly_demand")

hourly_df = (
    hourly_df
    .withColumn("hour", hour("demand_hour"))
    .withColumn("day_of_week", dayofweek("demand_hour"))
    .withColumn("day_of_month", dayofmonth("demand_hour"))
    .withColumn("month", month("demand_hour"))
)

window = Window.partitionBy("PULocationID").orderBy("demand_hour")

hourly_df = (
    hourly_df
    .withColumn("demand_lag_1h", lag("demand", 1).over(window))
    .withColumn("demand_lag_2h", lag("demand", 2).over(window))
    .withColumn("demand_lag_24h", lag("demand", 24).over(window))
    .dropna(subset=["demand_lag_1h", "demand_lag_2h", "demand_lag_24h"])
)

feature_cols = [
    "PULocationID",
    "hour",
    "day_of_week",
    "day_of_month",
    "month",
    "demand_lag_1h",
    "demand_lag_2h",
    "demand_lag_24h"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

ml_df = assembler.transform(hourly_df).select(
    "demand_hour",
    "PULocationID",
    "demand",
    "features"
)

train_df = ml_df.filter(col("demand_hour") < "2025-01-25")
test_df = ml_df.filter(col("demand_hour") >= "2025-01-25")

print("Training:", train_df.count())
print("Testing:", test_df.count())

# COMMAND ----------

import xgboost as xgb

print("XGBoost version:", xgb.__version__)

# COMMAND ----------

x_train = train_df.select("features", "demand").toPandas()
x_test = test_df.select("features", "demand").toPandas()

X_train = x_train["features"].apply(lambda x: x.toArray()).tolist()
y_train = x_train["demand"].values

X_test = x_test["features"].apply(lambda x: x.toArray()).tolist()
y_test = x_test["demand"].values

print("Training:", len(X_train))
print("Testing:", len(X_test))

# COMMAND ----------

import xgboost as xgb

xgb_model = xgb.XGBRegressor(
    n_estimators=200,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    objective="reg:squarederror",
    eval_metric="rmse",
    random_state=42
)

xgb_model.fit(X_train, y_train)

print("XGBoost training completed")

# COMMAND ----------

y_pred = xgb_model.predict(X_test)

print("Predictions generated:", len(y_pred))
print("First 10 predictions:")
print(y_pred[:10])

# COMMAND ----------

from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import numpy as np

rmse_xgb = np.sqrt(mean_squared_error(y_test, y_pred))
mae_xgb = mean_absolute_error(y_test, y_pred)
r2_xgb = r2_score(y_test, y_pred)

print("XGBoost RMSE:", rmse_xgb)
print("XGBoost MAE:", mae_xgb)
print("XGBoost R²:", r2_xgb)

# COMMAND ----------

prediction_pdf = test_df.select(
    "demand_hour",
    "PULocationID",
    "demand"
).toPandas()

prediction_pdf["prediction"] = y_pred

prediction_pdf.head(10)

# COMMAND ----------

prediction_spark_df = spark.createDataFrame(prediction_pdf)

prediction_spark_df.write.mode("overwrite").saveAsTable(
    "workspace.default.xgb_predictions"
)

print("Prediction table saved successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     PULocationID,
# MAGIC     ROUND(AVG(demand), 2) AS avg_actual_demand,
# MAGIC     ROUND(AVG(prediction), 2) AS avg_predicted_demand,
# MAGIC     ROUND(AVG(ABS(demand - prediction)), 2) AS avg_error
# MAGIC FROM workspace.default.xgb_predictions
# MAGIC GROUP BY PULocationID
# MAGIC ORDER BY avg_actual_demand DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     HOUR(demand_hour) AS hour,
# MAGIC     ROUND(AVG(demand), 2) AS avg_actual_demand,
# MAGIC     ROUND(AVG(prediction), 2) AS avg_predicted_demand
# MAGIC FROM workspace.default.xgb_predictions
# MAGIC GROUP BY HOUR(demand_hour)
# MAGIC ORDER BY hour;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     demand_hour,
# MAGIC     PULocationID,
# MAGIC     demand,
# MAGIC     prediction
# MAGIC FROM workspace.default.xgb_predictions
# MAGIC WHERE demand >= 50
# MAGIC ORDER BY demand DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.default.surge_demand AS
# MAGIC WITH threshold AS (
# MAGIC     SELECT
# MAGIC         percentile_approx(demand, 0.90) AS surge_threshold
# MAGIC     FROM workspace.default.xgb_predictions
# MAGIC )
# MAGIC SELECT
# MAGIC     p.*,
# MAGIC     t.surge_threshold,
# MAGIC     CASE
# MAGIC         WHEN p.demand >= t.surge_threshold THEN 1
# MAGIC         ELSE 0
# MAGIC     END AS surge_flag
# MAGIC FROM workspace.default.xgb_predictions p
# MAGIC CROSS JOIN threshold t;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     PULocationID,
# MAGIC     COUNT(*) AS surge_hours,
# MAGIC     ROUND(AVG(demand), 2) AS avg_surge_demand,
# MAGIC     MAX(demand) AS peak_demand
# MAGIC FROM workspace.default.surge_demand
# MAGIC WHERE surge_flag = 1
# MAGIC GROUP BY PULocationID
# MAGIC ORDER BY surge_hours DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     HOUR(demand_hour) AS hour,
# MAGIC     COUNT(*) AS surge_hours,
# MAGIC     ROUND(AVG(demand), 2) AS avg_surge_demand,
# MAGIC     MAX(demand) AS peak_demand
# MAGIC FROM workspace.default.surge_demand
# MAGIC WHERE surge_flag = 1
# MAGIC GROUP BY HOUR(demand_hour)
# MAGIC ORDER BY surge_hours DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.default.dashboard_summary AS
# MAGIC SELECT
# MAGIC     HOUR(demand_hour) AS hour,
# MAGIC     PULocationID,
# MAGIC     SUM(demand) AS total_demand,
# MAGIC     AVG(prediction) AS avg_predicted_demand,
# MAGIC     MAX(demand) AS peak_demand,
# MAGIC     SUM(surge_flag) AS surge_hours
# MAGIC FROM workspace.default.surge_demand
# MAGIC GROUP BY
# MAGIC     HOUR(demand_hour),
# MAGIC     PULocationID;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.default.dashboard_summary
# MAGIC ORDER BY total_demand DESC
# MAGIC LIMIT 20;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.default.ml_metrics AS
# MAGIC SELECT
# MAGIC     16.5152 AS RMSE,
# MAGIC     7.0889 AS MAE,
# MAGIC     0.9528 AS R2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.default.ml_metrics;