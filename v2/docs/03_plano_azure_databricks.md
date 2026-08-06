# Plano De Execucao Azure V2

Este documento e um roteiro operacional para recriar a infraestrutura Azure e
executar a V2 do projeto.

A logica principal ja esta nos scripts PySpark em `v2/pipelines`. Na cloud, o
objetivo e mudar infraestrutura, caminhos, secrets e orquestracao, sem reescrever
as regras de Bronze, Silver e Gold.

## Objetivo

Executar o pipeline completo de 2025 no Azure:

```text
NYC TLC + Taxi Zone Lookup + NOAA
  -> raw
  -> bronze
  -> silver
  -> gold
```

Pergunta analitica:

```text
As condicoes climaticas afetam a demanda por taxi em Nova York?
```

## Decisoes Da V2

- Ano de referencia: `2025`.
- Taxi: NYC TLC Yellow Taxi.
- Localizacao: Taxi Zone Lookup oficial da NYC TLC.
- Clima: NOAA GHCND.
- Escopo climatico: varias estacoes NOAA de NYC via `locationid=CITY:US360019`.
- Regra de modelagem: consolidar a NOAA para 1 linha por data antes de ligar com
  corridas.
- Granularidade climatica da V2.5: diaria. Todas as corridas de uma data usam o
  mesmo `clima_id` consolidado daquela data.
- Camadas: Raw, Bronze, Silver e Gold.
- Formato das camadas tratadas: Delta Lake.
- Orquestracao cloud: Azure Data Factory chamando notebooks Databricks.
- Paginacao NOAA: fica no codigo Python/Databricks, nao no ADF.

## Recursos Azure

Criar ou configurar:

```text
1. Resource Group
2. Storage Account com ADLS Gen2
3. Containers ou volumes para raw e delta
4. Azure Databricks Workspace
5. Databricks Cluster ou Jobs Cluster
6. Key Vault
7. Databricks Secret Scope
8. Azure Data Factory
9. Linked Services do ADF para Databricks
10. Pipeline ADF com atividades Databricks Notebook
```

## Deploy Do Codigo

Os notebooks em `v2/databricks/notebooks` importam as funcoes em
`v2/pipelines`. No Databricks, garanta que o repositorio esteja disponivel com a
pasta `v2` preservada.

Opcao mais simples para a primeira execucao:

```text
Databricks Git folders conectado ao GitHub
```

Alternativa futura:

```text
build/deploy como wheel no cluster ou job
```

## Caminhos Cloud

Opcao recomendada para os downloaders Python no Databricks:

```text
/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/taxi_zone_lookup
/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd_nyc/2025
/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/taxi_zone_lookup
/Volumes/<catalog>/<schema>/<volume>/delta/bronze/noaa/ghcnd_nyc/2025
/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/taxi_zone_lookup
/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd_nyc/2025
/Volumes/<catalog>/<schema>/<volume>/delta/gold/daily_weather_demand/2025
/Volumes/<catalog>/<schema>/<volume>/delta/gold/star_schema/2025
/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/nyc_tlc/taxi_zone_lookup
/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/nyc_tlc/taxi_zone_lookup
/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/noaa/ghcnd_nyc/2025
/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/noaa/ghcnd_nyc/2025
```

Alternativa:

```text
/dbfs/mnt/raw/...
/dbfs/mnt/delta/...
```

Evitar passar `abfss://...` diretamente para os scripts de ingestion Python,
porque eles escrevem arquivos via filesystem. `abfss://...` pode ser usado com
Spark nas camadas Bronze, Silver e Gold, se o cluster estiver configurado para
acessar o ADLS.

## Secrets

Localmente:

```text
.env
NOAA_TOKEN=...
```

No Azure:

```text
Key Vault
  -> noaa-token

Databricks Secret Scope
  -> scope: kv-lakehouse
  -> key: noaa-token
```

O token NOAA nao deve ir para:

- codigo;
- README;
- ADF em texto puro;
- parametros visiveis de pipeline;
- commit Git.

## Ordem Do Pipeline

Ordem recomendada no ADF:

```text
1. ingest_nyc_tlc
2. ingest_taxi_zone_lookup
3. ingest_noaa_weather
4. bronze_nyc_tlc
5. bronze_taxi_zone_lookup
6. bronze_noaa_weather
7. silver_nyc_tlc com Data Quality TLC
8. silver_taxi_zone_lookup com Data Quality Lookup
9. silver_noaa_weather com Data Quality NOAA
10. validate_silver_nyc_tlc
11. validate_silver_taxi_zone_lookup
12. validate_silver_noaa_weather
13. gold_star_schema
14. validate_gold_star_schema
15. gold_daily_weather_demand
16. validate_gold_daily_weather_demand
```

## Notebooks Databricks

Os notebooks ficam em:

```text
v2/databricks/notebooks/
```

Lista:

```text
ingest_nyc_tlc.py
ingest_taxi_zone_lookup.py
ingest_noaa_weather.py
bronze_nyc_tlc.py
bronze_taxi_zone_lookup.py
bronze_noaa_weather.py
silver_nyc_tlc.py
silver_taxi_zone_lookup.py
silver_noaa_weather.py
validate_silver_nyc_tlc.py
validate_silver_taxi_zone_lookup.py
validate_silver_noaa_weather.py
gold_daily_weather_demand.py
validate_gold_daily_weather_demand.py
gold_star_schema.py
validate_gold_star_schema.py
```

Eles sao wrappers. A regra de negocio fica em:

```text
v2/pipelines/
```

## Parametros ADF

### ingest_nyc_tlc

```text
year=2025
start_month=1
end_month=12
output=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/yellow/2025
overwrite=false
dry_run=false
```

### ingest_taxi_zone_lookup

```text
output=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/taxi_zone_lookup
overwrite=false
dry_run=false
```

### ingest_noaa_weather

```text
year=2025
locationid=CITY:US360019
output=/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd_nyc/2025
secret_scope=kv-lakehouse
secret_key=noaa-token
datasetid=GHCND
storage_datasetid=GHCND_NYC
datatypeids=PRCP,TMAX,TMIN,SNOW,SNWD
units=metric
limit=1000
initial_offset=1
overwrite=false
dry_run=false
```

### bronze_nyc_tlc

```text
year=2025
input=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/yellow/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/yellow/2025
mode=overwrite
skip_count=true
dry_run=false
```

### bronze_taxi_zone_lookup

```text
input=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/taxi_zone_lookup/taxi_zone_lookup.csv
output=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/taxi_zone_lookup
mode=overwrite
skip_count=true
dry_run=false
```

### bronze_noaa_weather

```text
year=2025
datasetid=GHCND_NYC
input=/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd_nyc/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/noaa/ghcnd_nyc/2025
mode=overwrite
skip_count=true
dry_run=false
```

### silver_nyc_tlc

```text
year=2025
input=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/yellow/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
quarantine_output=/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/nyc_tlc/yellow/2025
metrics_output=/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/nyc_tlc/yellow/2025
pipeline_run_id=<adf_pipeline_run_id>
skip_quality=false
mode=overwrite
start_date=2025-01-01
end_date=2026-01-01
skip_count=true
dry_run=false
```

### silver_taxi_zone_lookup

```text
input=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/taxi_zone_lookup
output=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/taxi_zone_lookup
quarantine_output=/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/nyc_tlc/taxi_zone_lookup
metrics_output=/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/nyc_tlc/taxi_zone_lookup
pipeline_run_id=<adf_pipeline_run_id>
skip_quality=false
mode=overwrite
skip_count=true
dry_run=false
```

### silver_noaa_weather

```text
year=2025
datasetid=GHCND_NYC
input=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/noaa/ghcnd_nyc/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd_nyc/2025
quarantine_output=/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/noaa/ghcnd_nyc/2025
metrics_output=/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/noaa/ghcnd_nyc/2025
pipeline_run_id=<adf_pipeline_run_id>
skip_quality=false
mode=overwrite
skip_count=false
dry_run=false
```

### validate_silver_nyc_tlc

```text
year=2025
input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
expected_days=365
min_rows=1
dry_run=false
```

### validate_silver_taxi_zone_lookup

```text
input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/taxi_zone_lookup
expected_locations=265
dry_run=false
```

### validate_silver_noaa_weather

```text
year=2025
datasetid=GHCND_NYC
input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd_nyc/2025
expected_days=365
min_rows=365
min_stations=2
dry_run=false
```

### gold_star_schema

```text
year=2025
tlc_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
noaa_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd_nyc/2025
taxi_zone_lookup_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/taxi_zone_lookup
output=/Volumes/<catalog>/<schema>/<volume>/delta/gold/star_schema/2025
mode=overwrite
skip_count=true
dry_run=false
```

### validate_gold_star_schema

```text
year=2025
input=/Volumes/<catalog>/<schema>/<volume>/delta/gold/star_schema/2025
expected_days=365
expected_locations=265
min_fact_rows=1
allow_incomplete_weather=false
dry_run=false
```

### gold_daily_weather_demand

```text
year=2025
tlc_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
noaa_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd_nyc/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/gold/daily_weather_demand/2025
mode=overwrite
skip_count=true
dry_run=false
```

### validate_gold_daily_weather_demand

```text
year=2025
input=/Volumes/<catalog>/<schema>/<volume>/delta/gold/daily_weather_demand/2025
expected_days=365
min_days_with_demand=1
min_total_trips=1
allow_incomplete_weather=false
dry_run=false
```

## Validacoes Apos Executar

Validar NOAA raw:

```text
downloaded_results == expected_count
distinct_dates == 365
data_min == 2025-01-01
data_max == 2025-12-31
```

Validar Silver NOAA:

```text
dias_distintos == 365
linhas > 365 quando houver varias estacoes
qtd_estacoes > 1
quality.pipeline_status == PASS ou WARNING
quality.invalid_records revisado quando maior que 0
```

Validar Silver TLC:

```text
linhas > 0
data_min >= 2025-01-01
data_max < 2026-01-01
quality.pipeline_status == PASS ou WARNING
quality.invalid_records revisado quando maior que 0
```

Validar Silver Taxi Zone Lookup:

```text
linhas > 0
location_id sem duplicidade
quality.pipeline_status == PASS
quality.invalid_records == 0
```

Validar Gold diaria:

```text
linhas == 365
dias_sem_clima == 0
registro_alinhamento_incompleto == 0
qtd_corridas_nula == 0
total_corridas > 0
```

Validar Gold Star Schema:

```text
dim_data.data_id sem duplicidade
dim_clima.clima_id sem duplicidade
dim_clima com 365 dias
dim_localizacao enriquecida com borough, zona e zona_servico
fact_trips.data_id_nulo == 0
fact_trips.clima_id_nulo == 0
fact_trips.localizacao_partida_id_nulo == 0
fact_trips.localizacao_chegada_id_nulo == 0
chaves_orfas == 0
```

## Execucao Recomendada

1. Rodar os notebooks com `dry_run=true` para conferir caminhos e parametros.
2. Rodar `ingest_taxi_zone_lookup`, Bronze Lookup e Silver Lookup.
3. Rodar `ingest_noaa_weather` e verificar se a paginacao baixou tudo.
4. Rodar `ingest_nyc_tlc` para os 12 meses.
5. Rodar Bronze NOAA e Silver NOAA primeiro, porque sao leves.
6. Rodar Bronze TLC.
7. Rodar Silver TLC com `skip_count=true`.
8. Rodar `validate_silver_nyc_tlc`, `validate_silver_taxi_zone_lookup` e
   `validate_silver_noaa_weather`.
9. Rodar Gold Star Schema com `skip_count=true`.
10. Rodar `validate_gold_star_schema`.
11. Rodar Gold diaria com `skip_count=true`.
12. Rodar `validate_gold_daily_weather_demand`.
13. Rodar queries de validacao exploratorias no Databricks se quiser investigar.
14. So depois pensar em OPTIMIZE, ZORDER, incremental e ML.

## Pontos De Atencao

- A Silver TLC completa e a Gold Star completa podem ser pesadas localmente.
- No Databricks, usar cluster com memoria suficiente para a TLC 2025 completa.
- `skip_count=true` evita disparar contagens caras durante processamento pesado.
- As Silvers TLC, Taxi Zone Lookup e NOAA ja executam Data Quality; em caso de
  `FAIL`, a Silver correspondente nao deve ser publicada.
- A Silver Taxi Zone Lookup deve ficar com `PASS`, porque e uma referencia pequena.
- A V2 ainda nao implementa pipeline incremental; atualmente o padrao e
  `overwrite`.
- `OPTIMIZE` e `ZORDER` devem ser aplicados depois da primeira execucao full,
  nao antes.

## Depois Da Primeira Execucao Full

Melhorias para uma segunda etapa:

```text
1. Validar a Gold full com Taxi Zone Lookup
2. Criar checks automatizados de qualidade da Gold
3. Definir particionamento fisico das tabelas Delta
4. Avaliar predictive optimization, liquid clustering ou OPTIMIZE/ZORDER
5. Planejar processamento incremental
6. Criar camada ML a partir da Gold diaria
```
