# Databricks notebook source
# MAGIC %run ../config_adls

# COMMAND ----------

from pyspark.sql.functions import explode, col, to_date, when

# COMMAND ----------


caminho_bronze = obter_caminho("bronze", "noaa", "bronze_delta", 2024)
caminho_silver = obter_caminho("silver", "noaa", "silver_delta", 2024)

df_bronze = spark.read.format("delta").load(caminho_bronze)

df_bronze.printSchema()


# COMMAND ----------

df_explode = df_bronze.select(
    explode(col("results")).alias("resultado")
)

# COMMAND ----------

df_organizar_colunas = df_explode.select(
    to_date(col("resultado.date")).alias("data"),
    col("resultado.station").alias("estacao"),
    col("resultado.datatype").alias("tipo_dado"),
    col("resultado.value").cast("double").alias("valor")
)

# COMMAND ----------

# remove valores nulos em estacao e valor.
df_sem_nulos = df_organizar_colunas.filter(
    col("valor").isNotNull() &
    col("estacao").isNotNull()
)

# COMMAND ----------

df_sem_duplicatas = df_sem_nulos.dropDuplicates([
    "data", "estacao", "tipo_dado"
])

display(df_sem_duplicatas)

# COMMAND ----------


df_renomear_tipo_dado = df_sem_duplicatas.withColumn(
    "tipo_dado",
    when(col("tipo_dado") == "PRCP", "precipitacao")
    .when(col("tipo_dado") == "TMAX", "temp_max")
    .when(col("tipo_dado") == "TMIN", "temp_min")
    .when(col("tipo_dado") == "SNOW", "neve")
    .when(col("tipo_dado") == "SNWD", "profundidade_neve")
    .otherwise(col("tipo_dado"))
)
display(df_renomear_tipo_dado)


# COMMAND ----------

df_clima = df_renomear_tipo_dado.filter(
    col("tipo_dado").isin(
        "temp_max",
        "temp_min",
        "precipitacao",
        "neve",
        "profundidade_neve"
    )
)
display(df_clima)

# COMMAND ----------

df_clima.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(caminho_silver)

print("Silver NOAA salva!")