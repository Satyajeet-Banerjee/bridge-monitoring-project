# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: 10-minute windowed metrics combining silver aggregates

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, window, max, avg, round


@dlt.table(
    name="bridge_metrics_gold",
    comment="10-minute average temperature, maximum vibration, and maximum tilt per bridge"
)
def bridge_metrics():

    # Read streaming silver tables with watermarking
    temp = (
        dlt.read_stream("bridge_temperature_silver")
            .withWatermark("event_time", "2 minutes")
    )

    vib = (
        dlt.read_stream("bridge_vibration_silver")
            .withWatermark("event_time", "2 minutes")
    )

    tilt = (
        dlt.read_stream("bridge_tilt_silver")
            .withWatermark("event_time", "2 minutes")
    )

    # Average temperature every 10 minutes
    temp_agg = (
        temp
            .groupBy(
                window("event_time", "10 minutes"),
                col("bridge_id"),
                col("name"),
                col("location")
            )
            .agg(
                avg("temperature").alias("avg_temperature")
            )
            .select(
                col("bridge_id"),
                col("name"),
                col("location"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("avg_temperature")
            )
    )

    # Maximum vibration every 10 minutes
    vib_agg = (
        vib
            .groupBy(
                window("event_time", "10 minutes"),
                col("bridge_id")
            )
            .agg(
                max("vibration").alias("max_vibration")
            )
            .select(
                col("bridge_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("max_vibration")
            )
    )

    # Maximum tilt every 10 minutes
    tilt_agg = (
        tilt
            .groupBy(
                window("event_time", "10 minutes"),
                col("bridge_id")
            )
            .agg(
                max("tilt_angle").alias("max_tilt_angle")
            )
            .select(
                col("bridge_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("max_tilt_angle")
            )
    )

    # Join all metrics together
    return (
        temp_agg.alias("t")
            .join(
                vib_agg.alias("v"),
                on=["bridge_id", "window_start", "window_end"],
                how="inner"
            )
            .join(
                tilt_agg.alias("l"),
                on=["bridge_id", "window_start", "window_end"],
                how="inner"
            )
            .select(
                col("bridge_id"),
                col("name"),
                col("location"),
                col("window_start"),
                col("window_end"),
                round(col("avg_temperature"), 2).alias("avg_temperature"),
                col("max_vibration"),
                col("max_tilt_angle")
            )
    )