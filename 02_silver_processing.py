# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: static metadata + enrichment of each stream with bridge_metadata

# COMMAND ----------

import dlt
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bridge Metadata (Static Table)

# COMMAND ----------

@dlt.table(
    name="bridge_metadata_silver",
    comment="Static metadata for five major European bridges"
)
def bridge_metadata():

    bridges = [
        {"bridge_id":1, "name":"Millau Viaduct", "length_m":2460,"main_span_m":342, "height_m":343, "location":"Tarn Valley, France", "type":"Cable-stayed viaduct", "opened_year": 2004},
        {"bridge_id":2, "name":"Vasco da Gama Bridge", "length_m":17280, "main_span_m":420, "height_m":155, "location":"Lisbon, Portugal", "type":"Box girder bridge", "opened_year": 1998},
        {"bridge_id":3, "name":"Øresund Bridge", "length_m":7845, "main_span_m":490, "height_m":204, "location":"Copenhagen–Malmö, Denmark/Sweden", "type":"Cable-stayed & tunnel","opened_year":2000},
        {"bridge_id":4, "name":"15 July Martyrs Bridge", "length_m":1560, "main_span_m":1074, "height_m":165, "location":"Istanbul, Turkey", "type":"Suspension bridge", "opened_year":1973},
        {"bridge_id":5, "name":"Forth Bridge", "length_m":2467, "main_span_m":521, "height_m":110, "location":"Firth of Forth, Scotland", "type":"Cantilever railway bridge","opened_year":1890},
    ]

    return spark.createDataFrame(bridges)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bridge Temperature (Streaming Table)

# COMMAND ----------

@dlt.table(
    name="bridge_temperature_silver",
    comment="Temperature enriched with metadata"
)
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dlt.expect("valid_temperature_range", "temperature BETWEEN -20 AND 60")
def silver_bridge_temperature():

    return (
        dlt.read_stream("bridge_temperature_bronze")
            .withColumn("event_time", col("event_time").cast("timestamp"))
            .withColumnRenamed("device_id", "bridge_id")
            .join(
                dlt.read("bridge_metadata_silver"),
                on="bridge_id",
                how="left"
            )
            .select(
                col("bridge_id"),
                col("name"),
                col("location"),
                col("event_time"),
                col("temperature")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bridge Vibration (Streaming Table)

# COMMAND ----------

@dlt.table(
    name="bridge_vibration_silver",
    comment="Vibration enriched with metadata"
)
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dlt.expect("valid_vibration_range", "vibration BETWEEN 0 AND 0.1")
def silver_bridge_vibration():

    return (
        dlt.read_stream("bridge_vibration_bronze")
            .withColumn("event_time", col("event_time").cast("timestamp"))
            .withColumnRenamed("device_id", "bridge_id")
            .join(
                dlt.read("bridge_metadata_silver"),
                on="bridge_id",
                how="left"
            )
            .select(
                col("bridge_id"),
                col("name"),
                col("location"),
                col("event_time"),
                col("vibration")
            )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bridge Tilt (Streaming Table)

# COMMAND ----------

@dlt.table(
    name="bridge_tilt_silver",
    comment="Tilt angle enriched with metadata"
)
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dlt.expect("valid_tilt_range", "tilt_angle BETWEEN -0.005 AND 0.005")
def silver_bridge_tilt():

    return (
        dlt.read_stream("bridge_tilt_bronze")
            .withColumn("event_time", col("event_time").cast("timestamp"))
            .withColumnRenamed("device_id", "bridge_id")
            .join(
                dlt.read("bridge_metadata_silver"),
                on="bridge_id",
                how="left"
            )
            .select(
                col("bridge_id"),
                col("name"),
                col("location"),
                col("event_time"),
                col("tilt_angle")
            )
    )