# Databricks notebook source
# MAGIC %run ../config_adls

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura das camadas Silver
# MAGIC Leitura dos dados de corridas da NYC TLC e dados climáticos da NOAA.

# COMMAND ----------


caminho_silver_noaa = obter_caminho("silver", "noaa", "silver_delta", 2024)
df_clima = spark.read.format("delta").load(caminho_silver_noaa)

caminho_silver_tlc = obter_caminho("silver", "nyc", "silver_delta", 2024)
df_taxi = spark.read.format("delta").load(caminho_silver_tlc)



# COMMAND ----------

df_clima.printSchema()
df_taxi.printSchema()

# COMMAND ----------

display(df_clima)

# COMMAND ----------

display(df_taxi)

# COMMAND ----------

from pyspark.sql.functions import row_number,avg,round
from pyspark.sql.window import Window
from pyspark.sql.functions import to_date, col,first, count
from pyspark.sql.functions import year, month, dayofmonth, dayofweek

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimensão de Clima
# MAGIC #### dim_clima
# MAGIC
# MAGIC Transformação dos dados meteorológicos da camada Silver para uma estrutura analítica na camada Gold, permitindo correlacionar condições climáticas com a demanda de corridas.

# COMMAND ----------

df_clima.count()

# COMMAND ----------



# Filtra apenas os tipos de dado meteorológico relevantes para a análise
tipos_importantes = ["precipitacao", "temp_max", "temp_min", "neve"]
df_clima_filtrado = df_clima.filter(col("tipo_dado").isin(tipos_importantes))
 
# Pivot: transforma os tipos de dado em colunas, agrupando por data e estação
# Resultado: 1 linha por (data, estacao) com colunas temp_max, temp_min, etc.
df_pivot = df_clima_filtrado.groupBy("data", "estacao") \
    .pivot("tipo_dado") \
    .agg(first("valor"))
 
# Preenche nulos com 0 para estações que não registraram algum tipo de dado
df_pivot = df_pivot.fillna(0)

# Remove registros sem temperatura válida
# Estações que retornaram 0 em temp_max e temp_min são dias sem medição real
df_pivot = df_pivot.filter(
    ~((col("temp_max") == 0) & (col("temp_min") == 0))
)

# Agrega por data: média de todas as estações disponíveis naquele dia
# Garante 1 linha por dia — evita multiplicação de linhas no join com a fact_trips
# Divide por 10 pois a NOAA fornece valores em décimos de unidade (ex: 130 = 13.0°C
df_dim_clima = df_pivot.groupBy("data").agg(
    round(avg("precipitacao"), 2).alias("precipitacao"),
    round(avg("temp_max") , 2).alias("temp_max"),
    round(avg("temp_min") , 2).alias("temp_min"),
    round(avg("neve") , 2).alias("neve")
)

# Cria a chave PK ordenada por data
window = Window.orderBy("data")
df_dim_clima = df_dim_clima.withColumn(
    "clima_id",
    row_number().over(window)
)
 
# Seleciona e ordena as colunas finais da dimensão
df_dim_clima = df_dim_clima.select(
    "clima_id",
    "data",
    "precipitacao",
    "temp_max",
    "temp_min",
    "neve"
)
 
display(df_dim_clima)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimensão de Data 
# MAGIC #### dim_date
# MAGIC
# MAGIC Criação da dimensão de tempo a partir das datas presentes nas corridas, permitindo análises temporais como corridas por dia, mês, ano e sazonalidade.

# COMMAND ----------

display(df_taxi)

# COMMAND ----------

# Extrai datas únicas das corridas, filtra apenas o ano de 2024
# Evita datas inválidas vindas da silver.
df_data = (
    df_taxi
    .withColumn("data", to_date("data_hora_partida"))
    .filter(year("data") == 2024) 
    .select("data")
    .distinct()
)

# Cria atributos de tempo da data
# Permite análises por ano, mês, dia e dia da semana
df_dim_data = df_data \
    .withColumn("ano", year("data")) \
    .withColumn("mes", month("data")) \
    .withColumn("dia", dayofmonth("data")) \
    .withColumn("dia_semana", dayofweek("data"))
 
# Gera a chave primária ordenada por data
window = Window.orderBy("data")
df_dim_data = df_dim_data.withColumn(
    "id_data",
    row_number().over(window)
)

# Seleciona e ordena as colunas finais da dimensão
df_dim_data = df_dim_data.select(
    "id_data",
    "data",
    "ano",
    "mes",
    "dia",
    "dia_semana"
)
 
display(df_dim_data)
 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimensão de localização
# MAGIC #### dim_localização
# MAGIC
# MAGIC Construção da dimensão de localização a partir das zonas de partida e chegada das corridas, permitindo análises geográficas da mobilidade urbana em NYC.

# COMMAND ----------

# Extrai os IDs de localização de partida e chegada como colunas separadas
df_partida = df_taxi.selectExpr("ID_local_partida as location_id")
df_chegada = df_taxi.selectExpr("ID_local_chegada as location_id")
 
# Une partida e chegada e remove duplicatas
# Garante que todas as zonas usadas nas corridas estejam representadas
df_locations = df_partida.union(df_chegada).distinct()
 

# Gera chave PK ordenada por location_id
window = Window.orderBy("location_id")
df_dim_localizacao = df_locations.withColumn(
    "id_localizacao",
    row_number().over(window)
)

# Seleciona as colunas finais da dimensão
df_dim_localizacao = df_dim_localizacao.select(
    "id_localizacao",
    "location_id"
)
 
display(df_dim_localizacao)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabela Fato de Corridas
# MAGIC #### fact_trips
# MAGIC
# MAGIC Tabela fato que consolida as corridas de táxi, conectando as dimensões data, localização e clima, e armazenando métricas como distância, valor da corrida e duração da viagem.

# COMMAND ----------

# Extrai a data da hora de partida para usar como chave de join com dim_data e dim_clima
df_fact = df_taxi.withColumn(
    "data",
    to_date(col("data_hora_partida"))
)

# Join com dim_data para trazer o id_data (FK)
# Left join para manter todas as corridas, mesmo sem data correspondente na dimensão
df_fact = df_fact.join(
    df_dim_data.select("id_data", "data"),
    on="data",
    how="left"
)

# Join com dim_localizacao para trazer o id da zona de partida (FK)
# Renomeia as colunas antes do join para evitar conflito de nomes
df_fact = df_fact.join(
    df_dim_localizacao
        .withColumnRenamed("location_id", "ID_local_partida")
        .withColumnRenamed("id_localizacao", "id_localizacao_partida"),
    on="ID_local_partida",
    how="left"
)

# Join com dim_localizacao para trazer o id da zona de chegada (FK)
# Reutiliza a mesma dimensão com renomeação diferente
df_fact = df_fact.join(
    df_dim_localizacao
        .withColumnRenamed("location_id", "ID_local_chegada")
        .withColumnRenamed("id_localizacao", "id_localizacao_chegada"),
    on="ID_local_chegada",
    how="left"
)

# Join com dim_clima pela data — garante 1 linha por data, sem multiplicação de linhas
df_fact = df_fact.join(
    df_dim_clima.select("clima_id", "data"),
    on="data",
    how="left"
)

# Seleciona apenas as chaves estrangeiras e métricas da tabela fato
df_fact_trips = df_fact.select(
    "id_data",
    "id_localizacao_partida",
    "id_localizacao_chegada",
    "clima_id",
    "distancia_viagem",
    "valor_total"
)

# Validação da fact_trips antes de salvar
# Verifica contagem total, nulos no clima e valores negativos impossíveis

print("Total linhas fact_trips  :", df_fact_trips.count())
print("Clima nulo               :", df_fact_trips.filter(col("clima_id").isNull()).count())
print("Distância negativa       :", df_fact_trips.filter(col("distancia_viagem") < 0).count())
print("Valor total negativo     :", df_fact_trips.filter(col("valor_total") < 0).count())

df_fact_trips.select(
    "id_data",
    "id_localizacao_partida",
    "id_localizacao_chegada",
    "clima_id"
).summary("count", "min", "max").display()
display(df_fact_trips)

# COMMAND ----------


# Salvamento na Gold
caminho_gold = obter_caminho("gold", "nyc", "gold_delta", 2024)

df_dim_data.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(caminho_gold + "/dim_data")

df_dim_localizacao.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(caminho_gold + "/dim_localizacao")

df_dim_clima.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(caminho_gold + "/dim_clima")

df_fact_trips.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(caminho_gold + "/fact_trips")

print("Gold salva com sucesso!")
print(f"Path: {caminho_gold}")

# COMMAND ----------

import os

os.makedirs("/tmp/gold", exist_ok=True)

df_dim_data.toPandas().to_csv("/tmp/gold/dim_data.csv", index=False)
df_dim_localizacao.toPandas().to_csv("/tmp/gold/dim_localizacao.csv", index=False)
df_dim_clima.toPandas().to_csv("/tmp/gold/dim_clima.csv", index=False)

print("CSVs exportados!")

# COMMAND ----------

# Verifica se salvou
import os
os.listdir("/tmp/gold")

# COMMAND ----------

dbutils.fs.cp("file:/tmp/gold/dim_data.csv", "abfss://gold@pipelinelakehouse.dfs.core.windows.net/csv/dim_data.csv")
dbutils.fs.cp("file:/tmp/gold/dim_localizacao.csv", "abfss://gold@pipelinelakehouse.dfs.core.windows.net/csv/dim_localizacao.csv")
dbutils.fs.cp("file:/tmp/gold/dim_clima.csv", "abfss://gold@pipelinelakehouse.dfs.core.windows.net/csv/dim_clima.csv")

# COMMAND ----------

df_fact_trips.coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("file:/tmp/gold/fact_trips_csv")

# COMMAND ----------

dbutils.fs.cp("file:/tmp/gold/fact_trips_csv", "abfss://gold@pipelinelakehouse.dfs.core.windows.net/csv/fact_trips_csv", recurse=True)