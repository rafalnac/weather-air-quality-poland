-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### SQL notebook for creation objects like catalogs, schemas etc.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS project_weather_air;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS project_weather_air.air_quality;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS project_weather_air.weather_data;
