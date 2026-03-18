# Databricks notebook source
# MAGIC %run ../config_adls

# COMMAND ----------


caminho_leitura = obter_caminho("bronze", "noaa", "bronze", 2024)   
caminho_delta = obter_caminho("bronze", "noaa", "bronze_delta", 2024)    

df_bronze = spark.read.json(caminho_leitura)

df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(caminho_delta)

df_bronze.printSchema()
display(df_bronze)
df_bronze.count()