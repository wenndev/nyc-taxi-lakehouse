# Arquitetura Azure V2 / V2.5

Referencia historica/alternativa Azure. O destino atual planejado e AWS,
descrito em `09_migracao_aws_local_first.md`. Este desenho nao representa uma
infraestrutura V2 atualmente implantada.

Este documento representa a arquitetura planejada para executar a V2 no Azure.
A ideia nao e refazer o projeto do zero, mas levar para a cloud a logica ja
validada localmente em PySpark, Delta Lake, Data Quality e Gold.

## Diagrama Oficial

O diagrama da arquitetura fica somente em Excalidraw:

```text
v2/docs/arquitetura_azure_v2.excalidraw
```

## Leitura Do Diagrama

O fluxo principal continua sendo Medallion:

```text
fontes externas
  -> ADF
  -> Databricks
  -> raw
  -> bronze
  -> silver
  -> gold
  -> consumo
```

O ADF nao deve conter a regra de negocio. Ele deve orquestrar a ordem das
etapas, passar parametros e controlar falhas.

O Databricks executa os notebooks em `v2/databricks/notebooks`, que chamam a
logica reutilizavel em `v2/pipelines`.

O ADLS Gen2 guarda os arquivos e tabelas:

```text
raw:
  arquivos originais baixados das fontes

bronze:
  Delta quase bruto, com pouca transformacao

silver:
  limpeza, padronizacao, deduplicacao e Data Quality

quarantine:
  registros invalidos separados pela Data Quality

monitoring:
  metricas de qualidade e rastreabilidade

gold:
  Star Schema e base diaria clima x demanda
```

## Diferencas Em Relacao A V1

A arquitetura base e parecida com a V1, mas a V2 adiciona maturidade operacional:

```text
1. NOAA com paginacao por offset.
2. Retry/backoff/jitter no consumo NOAA.
3. Varias estacoes NOAA de NYC.
4. Consolidacao climatica para 1 linha por data.
5. Data Quality antes de publicar Silver.
6. Quarantine para registros invalidos.
7. Metricas de qualidade.
8. Validadores Silver e Gold.
9. Particionamento Delta por ano e mes.
10. Backfill mensal com replace_month e replaceWhere.
11. Gold daily_weather_demand para EDA/ML.
```

## Responsabilidade De Cada Servico

| Servico | Papel Na Arquitetura |
|---|---|
| Azure Data Factory | Orquestrar jobs, passar parametros, controlar ordem e falhas. |
| Azure Databricks | Rodar PySpark, Delta Lake, validacoes e transformacoes. |
| ADLS Gen2 | Armazenar raw, bronze, silver, gold, quarantine e monitoring. |
| Key Vault | Guardar o token NOAA fora do codigo. |
| Databricks Secret Scope | Permitir que Databricks leia o token do Key Vault. |
| Databricks SQL | Consultar Gold e servir dados para analise. |
| Power BI | Visualizar indicadores finais. |

## Ordem Operacional

```text
1. ingest_nyc_tlc
2. ingest_taxi_zone_lookup
3. ingest_noaa_weather
4. bronze_nyc_tlc
5. bronze_taxi_zone_lookup
6. bronze_noaa_weather
7. silver_nyc_tlc
8. silver_taxi_zone_lookup
9. silver_noaa_weather
10. validate_silver_nyc_tlc
11. validate_silver_taxi_zone_lookup
12. validate_silver_noaa_weather
13. gold_star_schema
14. validate_gold_star_schema
15. gold_daily_weather_demand
16. validate_gold_daily_weather_demand
```

## DataOps Na Arquitetura

A V2 ja entra na cloud com alguns mecanismos de confiabilidade:

```text
NOAA:
  retry/backoff/jitter
  manifest com downloaded_results e expected_count

Silver:
  Data Quality
  quarantine
  metrics

Gold:
  validadores de chaves, datas e clima

Backfill:
  replace_month
  replaceWhere

Observabilidade:
  pipeline_run_id
  logs
  metricas de qualidade
```

No Azure, isso deve ser complementado com:

```text
ADF retry policy
Databricks Job retries
Azure Monitor
alertas de falha
alertas de custo
```

## Caminhos Cloud Esperados

```text
/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/taxi_zone_lookup
/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd_nyc/2025

/Volumes/<catalog>/<schema>/<volume>/delta/bronze/...
/Volumes/<catalog>/<schema>/<volume>/delta/silver/...
/Volumes/<catalog>/<schema>/<volume>/delta/gold/...
/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/...
/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/...
```

## Resumo

A arquitetura Azure da V2 e a evolucao natural da V1:

```text
mesma base lakehouse
mais qualidade
mais confiabilidade
mais rastreabilidade
mais preparada para BI e ML
```
