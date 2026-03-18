# Databricks notebook source
# MAGIC %run ./config_adls

# COMMAND ----------

# MAGIC %run ./bronze/bronze_noaa_weather_2024

# COMMAND ----------

# MAGIC
# MAGIC %run ./bronze/bronze_nyc_tlc_2024

# COMMAND ----------

# MAGIC %run ./silver/silver_noaa_weather_2024

# COMMAND ----------

# MAGIC %run ./silver/silver_nyc_tlc_yellow_2024

# COMMAND ----------

# MAGIC %run ./gold/clima_taxi