# Databricks notebook source
# MAGIC %run ../config_adls

# COMMAND ----------

from pyspark.sql.functions import col, when

# COMMAND ----------

caminho_silver = obter_caminho("silver","nyc", "silver_delta", 2024)
caminho_bronze = obter_caminho("bronze", "nyc", "bronze_delta", 2024)
df_bronze = spark.read.format("delta").load(caminho_bronze)

# COMMAND ----------

print("Total Bronze:", df_bronze.count())


# COMMAND ----------

# MAGIC %md
# MAGIC mta_tax = É a taxa da Metropolitan Transportation Authority (MTA) de NY. Taxa fixa cobrada em quase toda corrida.
# MAGIC
# MAGIC store_and_fwd_flag = Se o taxímetro ficou sem internet, ele armazenou a corrida e enviou depois.
# MAGIC

# COMMAND ----------

def renomear_colunas(df):
   # Renomeia colunas para português

    return df.withColumnRenamed("VendorID", "id_vendedor")\
        .withColumnRenamed("tpep_pickup_datetime", "data_hora_partida")\
        .withColumnRenamed("tpep_dropoff_datetime", "data_hora_chegada")\
        .withColumnRenamed("passenger_count", "qtd_passageiros")\
        .withColumnRenamed("trip_distance","distancia_viagem")\
        .withColumnRenamed("RatecodeID", "ID_tarifa")\
        .withColumnRenamed("store_and_fwd_flag", "flag_armazenado_e_enviado")\
        .withColumnRenamed("PULocationID", "ID_local_partida")\
        .withColumnRenamed("DOLocationID", "ID_local_chegada")\
        .withColumnRenamed("payment_type", "tipo_pagamento")\
        .withColumnRenamed("fare_amount", "valor_tarifa")\
        .withColumnRenamed("extra", "taxa_extra")\
        .withColumnRenamed("mta_tax", "taxa_mta_fixa")\
        .withColumnRenamed("tip_amount", "gorjeta")\
        .withColumnRenamed("tolls_amount", "valor_pedagios")\
        .withColumnRenamed("improvement_surcharge", "sobretaxa_melhoria")\
        .withColumnRenamed("total_amount", "valor_total")\
        .withColumnRenamed("congestion_surcharge", "sobretaxa_transito")\
        .withColumnRenamed("Airport_fee", "taxa_aeroporto")

# COMMAND ----------

def tratar_colunas_criticas(df):
    # Remove registros com valores nulos em colunas essenciais para a viagem.
    
    # 1. id_vendedor deve existir e ser > 0
    df = df.filter(col("id_vendedor").isNotNull() & (col("id_vendedor") > 0))

    # 2. data_hora_partida não pode ser nula
    df = df.filter(col("data_hora_partida").isNotNull())

    # 3. data_hora_chegada não pode ser nula
    df = df.filter(col("data_hora_chegada").isNotNull())

    # 4. chegada não pode ser antes da partida
    df = df.filter(col("data_hora_chegada") >= col("data_hora_partida"))

    # 5. local de partida válido
    df = df.filter(col("ID_local_partida").isNotNull() & (col("ID_local_partida") > 0))

    # 6. local de chegada válido
    df = df.filter(col("ID_local_chegada").isNotNull() & (col("ID_local_chegada") > 0))

    # 7. valor total precisa existir e ser positivo
    df = df.filter(col("valor_total").isNotNull() & (col("valor_total") > 0))

    return df

# COMMAND ----------

def tratar_valores_invalidos(df):
    
    # Remove registros com valores impossíveis:
     # - valores negativos
     # - data de chegada menor que data de partida
    
    return df.filter(
        (col("distancia_viagem") >= 0) &
        (col("valor_total") >= 0) &
        (col("valor_tarifa") >= 0) &
        (col("data_hora_chegada") >= col("data_hora_partida"))
    )

# COMMAND ----------

# percorre as colunas numericas e os valores que for null troca por 0
def tratar_numericas(df):
    colunas_numericas = [
        "qtd_passageiros","distancia_viagem","gorjeta","valor_pedagios",
        "taxa_extra","taxa_mta_fixa","sobretaxa_melhoria",
        "sobretaxa_transito","taxa_aeroporto"
    ]
    
    for c in colunas_numericas:
        df = df.withColumn(c, when(col(c).isNull(), 0).otherwise(col(c)))
    
    return df

# COMMAND ----------

def tratar_categoricas(df):
    # Padroniza colunas categóricas substituindo NULL por 'DESCONHECIDO'.
    return df.withColumn(
        "flag_armazenado_e_enviado",
        when(col("flag_armazenado_e_enviado").isNull(), "DESCONHECIDO")
        .otherwise(col("flag_armazenado_e_enviado"))
    )

# COMMAND ----------

# Remove linhas duplicadas considerando só essas colunas como chave.
def remover_duplicatas(df):
    return df.dropDuplicates([
        "id_vendedor",
        "data_hora_partida",
        "ID_local_partida",
        "ID_local_chegada",
        "valor_total"
    ])

# COMMAND ----------

def pipeline_silver(df):
    df = renomear_colunas(df)
    df = tratar_colunas_criticas(df)
    df = tratar_valores_invalidos(df)
    df = tratar_numericas(df)
    df = tratar_categoricas(df)
    df = remover_duplicatas(df)
    
    return df

# COMMAND ----------

df_silver = pipeline_silver(df_bronze)

print("Total Bronze:", df_bronze.count())
print("Total Silver:", df_silver.count())
print("Diferença:", df_bronze.count() - df_silver.count())


# COMMAND ----------

def auditoria_pipeline(df_bronze):
    
    total0 = df_bronze.count()
    print(f"Bronze: {total0}")

    df1 = renomear_colunas(df_bronze)
    total1 = df1.count()
    print(f"Após renomear colunas: {total1} | Removidos: {total0 - total1}")

    df2 = tratar_colunas_criticas(df1)
    total2 = df2.count()
    print(f"Após tratar colunas críticas: {total2} | Removidos: {total1 - total2}")

    df3 = tratar_valores_invalidos(df2)
    total3 = df3.count()
    print(f"Após tratar valores inválidos: {total3} | Removidos: {total2 - total3}")

    df4 = tratar_numericas(df3)
    total4 = df4.count()
    print(f"Após tratar numéricas: {total4} | Removidos: {total3 - total4}")

    df5 = tratar_categoricas(df4)
    total5 = df5.count()
    print(f"Após tratar categóricas: {total5} | Removidos: {total4 - total5}")

    df6 = remover_duplicatas(df5)
    total6 = df6.count()
    print(f"Após remover duplicatas: {total6} | Removidos: {total5 - total6}")

    print("==================")
    print(f"Total removido no pipeline: {total0 - total6}")

    return df6

# COMMAND ----------

df_silver_auditado = auditoria_pipeline(df_bronze)

total_silver = df_silver_auditado.count()
print(f"Total Silver final (auditoria): {total_silver}")

display(df_silver_auditado)

# COMMAND ----------


df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(caminho_silver)

print("Silver salvo como Delta!")
print(f"Path: {caminho_silver}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Evolução do tratamento de dados (Pipeline Silver)
# MAGIC
# MAGIC Durante a fase inicial de implementação da lógica de tratamento de dados, o volume foi reduzido de aproximadamente **41 milhões para 37 milhões de registros**.  
# MAGIC Essa redução ocorreu principalmente devido a:
# MAGIC - aplicação direta de `dropna()` em múltiplas colunas,
# MAGIC - filtros ainda em fase de ajuste para colunas do tipo `timestamp`,
# MAGIC - ausência de regras específicas por tipo de dado.
# MAGIC
# MAGIC Após refinar a lógica do pipeline e implementar validações por coluna (colunas críticas, numéricas e categóricas), além de um processo de auditoria por etapa, o pipeline passou a:
# MAGIC - remover apenas registros realmente inválidos,
# MAGIC - preservar maior quantidade de dados úteis,
# MAGIC - melhorar a qualidade e a confiabilidade da camada Silver,
# MAGIC - permitir rastreabilidade das remoções realizadas em cada etapa.