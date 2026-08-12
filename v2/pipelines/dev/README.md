# Dev Pipeline V2

Esta pasta guarda comandos de apoio para validar o projeto localmente.

Eles nao substituem os scripts oficiais de cada camada. A ideia aqui e
orquestrar um fluxo pequeno para provar que Bronze, Silver, Quality e Gold estao
funcionando antes de levar o projeto para Azure/Databricks.

## Quando Usar

Use antes de commitar uma mudanca relevante ou antes de partir para cloud:

```bash
poetry run run-v2-dev-sample
```

## O Que Ele Executa

```text
Bronze NYC TLC sample
  -> Silver NYC TLC sample com Data Quality
  -> Validate Silver TLC sample
  -> Validate Silver Taxi Zone Lookup
  -> Validate Silver NOAA
  -> Gold Star Schema dev
  -> Validate Gold Star Schema dev
  -> Gold Daily Weather Demand dev
  -> Validate Gold Daily Weather Demand dev
```

## Entradas Necessarias

O comando assume que estas bases ja existem localmente:

```text
v2/data/raw/nyc_tlc/yellow/2025/yellow_tripdata_2025-01.parquet
v2/data/delta/silver/nyc_tlc/taxi_zone_lookup
v2/data/delta/silver/noaa/ghcnd_nyc/2025
```

Se alguma delas nao existir, rode antes as etapas de ingestion, bronze e silver
da respectiva fonte.

## Por Que Nao Container Agora

Neste momento, o objetivo e validar a logica local com o minimo de atrito.

```text
Poetry + PySpark local = suficiente para validar a V2 antes do Azure
Container = opcional depois, para padronizar ambiente
Kubernetes = desnecessario para este projeto agora
Terraform = entra quando a infraestrutura Azure for recriada
```

