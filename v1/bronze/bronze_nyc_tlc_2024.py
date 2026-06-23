# Databricks notebook source
# MAGIC %run ../config_adls

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

caminho_leitura = obter_caminho("bronze", "nyc", "bronze", 2024)
caminho_delta   = obter_caminho("bronze", "nyc", "bronze_delta", 2024)
df_bronze = spark.read.parquet(caminho_leitura)

display(df_bronze)


# COMMAND ----------

# transforma 3 colunas que estão "long" para "integer"
df_bronze = df_bronze \
    .withColumn("passenger_count", col("passenger_count").cast(IntegerType())) \
    .withColumn("RatecodeID", col("RatecodeID").cast(IntegerType())) \
    .withColumn("payment_type", col("payment_type").cast(IntegerType()))


# COMMAND ----------

df_bronze.write.format("delta").mode("overwrite").save(caminho_delta)

print("Bronze salvo como Delta!")
print(f"Path: {caminho_delta}")
df_bronze.printSchema()