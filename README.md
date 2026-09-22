# Ride-Sharing Demand & Surge Analytics

An end-to-end Big Data analytics project using NYC TLC Yellow Taxi trip data to analyze ride demand, identify demand surges, and predict short-term pickup demand using machine learning.

## Project Overview

This project demonstrates a practical Big Data workflow using Databricks, PySpark, Spark SQL, XGBoost, and Power BI.

The analysis focuses on:

- Ride demand by hour and pickup location
- Demand patterns across the day and week
- Short-term demand prediction
- Actual vs predicted demand
- Identification of high-demand surge periods
- Interactive business dashboarding

## Technology Stack

- **Databricks** — Cloud-based data engineering and analytics
- **PySpark** — Large-scale data processing
- **Spark SQL** — Data analysis and aggregation
- **Parquet** — Columnar data storage
- **XGBoost** — Demand prediction
- **Power BI** — Dashboard and visualization
- **Python** — Data processing and machine learning

## Dataset

**NYC TLC Yellow Taxi Trip Records — January 2025**

- Raw records: 3,475,226
- Cleaned records: 3,324,020
- Format: Parquet

Dataset source:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

The raw dataset is not included in this repository.

See [`data/README.md`](data/README.md) for dataset details.

## Data Processing

The raw taxi data was loaded into Databricks and processed using PySpark.

Cleaning included:

- Removing invalid trip distances
- Removing invalid total amounts
- Removing invalid trip durations
- Creating pickup date and time features
- Aggregating trips into location-hour demand records

The processed data was stored as Parquet and Delta/Unity Catalog tables in Databricks.

## Big Data Workflow

```text
NYC TLC Taxi Data
        ↓
Databricks
        ↓
PySpark Data Cleaning
        ↓
Feature Engineering
        ↓
Spark SQL Analytics
        ↓
Hourly Location-Level Demand
        ↓
Machine Learning
        ↓
XGBoost Demand Prediction
        ↓
Surge Detection
        ↓
Power BI Dashboard
```

## Machine Learning

Hourly demand was predicted using time-based and historical demand features.

### Features

- Pickup Location
- Hour
- Day of Week
- Day of Month
- Month
- Previous 1-hour demand
- Previous 2-hour demand
- Previous 24-hour demand

A chronological train/test split was used to avoid using future observations to predict the past.

### Model Results

| Model | RMSE | MAE | R² |
|---|---:|---:|---:|
| Random Forest | 21.53 | 8.17 | 0.920 |
| XGBoost | 16.52 | 7.09 | 0.953 |

The XGBoost model was used for the final prediction analysis.

> R² is reported as a model evaluation metric; it is not classification accuracy.

## Surge Detection

A demand surge threshold was calculated using the 90th percentile of test-set demand.

A location-hour record was marked as a surge event when its observed demand reached or exceeded this threshold.

This enables analysis of:

- High-demand hours
- High-demand pickup locations
- Peak demand periods
- Number of surge location-hours

## Power BI Dashboard

The Power BI dashboard contains two main sections:

### Page 1 — Demand Overview

- Total demand
- Peak hourly demand
- Average hourly demand
- Active pickup locations
- Hourly demand trend
- Demand by hour
- Top pickup locations

### Page 2 — Machine Learning & Surge Analysis

- XGBoost RMSE
- XGBoost MAE
- XGBoost R²
- Surge location-hours
- Actual vs predicted demand
- Surge analysis by hour and pickup location

## Repository Structure

```text
ride-sharing-demand-surge-analytics/
│
├── README.md
│
├── notebooks/
│   └── 01_Ride_Sharing_Demand_Analysis.py
│
├── sql/
│   ├── demand_analysis.sql
│   ├── ml_analysis.sql
│   └── surge_analysis.sql
│
├── dashboard/
│   └── Ride_Sharing_Demand_Analytics.pbix
│
├── screenshots/
│   ├── overview.png
│   └── ml_surge.png
│
└── data/
    └── README.md
```

## Reproducibility

1. Download the January 2025 NYC TLC Yellow Taxi Parquet dataset.
2. Upload the dataset to Databricks.
3. Run the PySpark notebook in `notebooks/`.
4. Run the SQL queries in `sql/`.
5. Train the demand prediction model.
6. Generate the prediction and surge tables.
7. Connect Power BI to the Databricks tables.
8. Open the Power BI dashboard.

## Key Skills Demonstrated

- Big Data processing
- Databricks
- PySpark
- Spark SQL
- Data cleaning
- Feature engineering
- Window functions and lag features
- Machine learning
- Time-based train/test splitting
- Demand forecasting
- Surge detection
- Power BI dashboard development
- Data pipeline design

## Author

**Anuj Kumawat**

Data Analytics & Big Data Project
