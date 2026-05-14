# Bridge Monitoring Streaming Pipeline

## Overview
This project is a real-time streaming data engineering pipeline built using Databricks, PySpark Structured Streaming, Delta Lake, and Lakeflow Declarative Pipelines (DLT).

The pipeline simulates IoT bridge monitoring sensors and processes streaming sensor data using a Medallion Architecture approach.

The system ingests:
- Bridge temperature readings
- Bridge vibration readings
- Bridge tilt-angle readings

and transforms them into analytics-ready datasets for monitoring and operational insights.

> Note: The sensor data used in this project is synthetically generated (fake/mock data) for learning and simulation purposes.

---

## Architecture

Streaming Sensors → Landing → Bronze → Silver → Gold

---

## Data Sources

### Temperature Sensors
Captures bridge temperature readings over time.

### Vibration Sensors
Monitors bridge vibration patterns and structural activity.

### Tilt Sensors
Tracks bridge tilt angles for structural stability analysis.

---

## Medallion Architecture

### Bronze Layer
- Raw streaming ingestion
- Delta stream processing
- Minimal transformation

### Silver Layer
- Data cleansing
- Standardization
- Schema refinement
- Enrichment

### Gold Layer
- Aggregated metrics
- Monitoring analytics
- Operational insights

---

## Technologies Used

- Databricks
- PySpark
- Structured Streaming
- Delta Lake
- Unity Catalog
- Lakeflow Declarative Pipelines (DLT)
- Azure Databricks

---

## Project Structure

```text
bridge-monitoring-project/
│
├── 00_data_generator/
│   └── Generates synthetic streaming sensor data
│
├── 01_bronze_processing/
│   └── Raw streaming ingestion and Bronze layer processing
│
├── 02_silver_processing/
│   └── Data cleansing, transformation, and enrichment logic
│
├── 03_gold_processing/
│   └── Aggregated analytics and monitoring metrics
│
├── queries.sql
└── README.md
```

---

## Streaming Pipeline Flow

1. Synthetic sensor data generation
2. Streaming ingestion into landing zone
3. Bronze Delta stream creation
4. Silver transformations and cleansing
5. Gold analytics generation

---

## Key Features

- Real-time streaming ingestion
- Structured Streaming pipelines
- Delta Lake storage
- Medallion architecture implementation
- Modular ETL design
- IoT-style sensor simulation
- Synthetic data generation for testing and learning

---

## Sample Use Cases

- Structural health monitoring
- Real-time operational analytics
- Predictive maintenance preparation
- Infrastructure monitoring pipelines

---

## Learning Outcomes

This project helped in understanding:
- Structured Streaming
- Delta streaming pipelines
- Real-time ETL architecture
- Lakehouse concepts
- Streaming medallion architecture
- DLT pipeline design

---
