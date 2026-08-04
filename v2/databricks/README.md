# Databricks V2

Wrappers para executar a V2 no Azure Databricks.

Esses arquivos nao concentram regra de negocio. Eles apenas:

- recebem parametros do ADF ou Databricks Job;
- buscam secrets no Databricks quando necessario;
- chamam os scripts em `v2/pipelines`.

## Notebooks

```text
v2/databricks/notebooks/ingest_nyc_tlc.py
v2/databricks/notebooks/ingest_taxi_zone_lookup.py
v2/databricks/notebooks/ingest_noaa_weather.py
v2/databricks/notebooks/bronze_nyc_tlc.py
v2/databricks/notebooks/bronze_taxi_zone_lookup.py
v2/databricks/notebooks/bronze_noaa_weather.py
v2/databricks/notebooks/silver_nyc_tlc.py
v2/databricks/notebooks/silver_taxi_zone_lookup.py
v2/databricks/notebooks/silver_noaa_weather.py
v2/databricks/notebooks/gold_daily_weather_demand.py
v2/databricks/notebooks/validate_gold_daily_weather_demand.py
v2/databricks/notebooks/gold_star_schema.py
v2/databricks/notebooks/validate_gold_star_schema.py
```

## Ordem Do Pipeline

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
10. gold_daily_weather_demand
11. validate_gold_daily_weather_demand
12. gold_star_schema
13. validate_gold_star_schema
```

Os notebooks de Bronze, Silver e Gold reutilizam as funcoes PySpark em
`v2/pipelines`. Eles nao devem concentrar regra de negocio.

## Deploy No Databricks

Os notebooks importam modulos como `v2.pipelines...`. Portanto, no Databricks o
repositorio deve ser disponibilizado preservando a pasta `v2`.

Opcoes:

```text
Databricks Git folders apontando para este repositorio Git
```

ou:

```text
deploy do projeto como wheel/package no cluster
```

Para esta V2, o caminho mais simples e usar Databricks Git folders durante a primeira
execucao cloud.

### Ingest NYC TLC

Responsabilidade:

- receber ano, intervalo de meses e caminho raw;
- baixar os Parquets publicos da NYC TLC;
- salvar os arquivos no raw da cloud.

Parametros principais no ADF:

```text
year
start_month
end_month
output
overwrite
dry_run
```

### Ingest NOAA Weather

Responsabilidade:

- receber ano, escopo NOAA e caminho raw;
- buscar token NOAA via `dbutils.secrets`;
- executar a ingestao NOAA com paginacao por `offset`;
- salvar paginas JSON no raw da cloud.

Parametros principais no ADF:

```text
year
stationid
locationid
output
storage_datasetid
secret_scope
secret_key
```

Exemplo V2.5:

```text
year=2025
locationid=CITY:US360019
output=/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd_nyc/2025
storage_datasetid=GHCND_NYC
secret_scope=kv-lakehouse
secret_key=noaa-token
```

### Ingest Taxi Zone Lookup

Responsabilidade:

- baixar o CSV oficial Taxi Zone Lookup;
- salvar o arquivo no raw da cloud.

Parametros principais no ADF:

```text
output
overwrite
dry_run
```

Exemplo:

```text
output=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/taxi_zone_lookup
```

### Bronze NYC TLC

Parametros principais no ADF:

```text
year
input
output
mode
limit
skip_count
dry_run
```

Exemplo:

```text
input=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/yellow/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/yellow/2025
```

### Bronze NOAA Weather

Parametros principais no ADF:

```text
year
datasetid
input
output
mode
skip_count
dry_run
```

Exemplo:

```text
input=/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd_nyc/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/noaa/ghcnd_nyc/2025
```

### Bronze Taxi Zone Lookup

Parametros principais no ADF:

```text
input
output
mode
skip_count
dry_run
```

Exemplo:

```text
input=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/taxi_zone_lookup/taxi_zone_lookup.csv
output=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/taxi_zone_lookup
```

### Silver NYC TLC

Parametros principais no ADF:

```text
year
input
output
quarantine_output
metrics_output
pipeline_run_id
skip_quality
mode
start_date
end_date
limit
skip_count
dry_run
```

Exemplo:

```text
input=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/yellow/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
quarantine_output=/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/nyc_tlc/yellow/2025
metrics_output=/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/nyc_tlc/yellow/2025
skip_quality=false
```

Se o Data Quality retornar `FAIL`, a Silver TLC nao e publicada e o job deve
falhar de forma controlada.

### Silver Taxi Zone Lookup

Parametros principais no ADF:

```text
input
output
quarantine_output
metrics_output
pipeline_run_id
skip_quality
mode
skip_count
dry_run
```

Exemplo:

```text
input=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/taxi_zone_lookup
output=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/taxi_zone_lookup
quarantine_output=/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/nyc_tlc/taxi_zone_lookup
metrics_output=/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/nyc_tlc/taxi_zone_lookup
skip_quality=false
```

Se o Data Quality retornar `FAIL`, a Silver Taxi Zone Lookup nao e publicada.

### Silver NOAA Weather

Parametros principais no ADF:

```text
year
datasetid
input
output
quarantine_output
metrics_output
pipeline_run_id
skip_quality
mode
skip_count
dry_run
```

Exemplo:

```text
input=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/noaa/ghcnd_nyc/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd_nyc/2025
quarantine_output=/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/noaa/ghcnd_nyc/2025
metrics_output=/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/noaa/ghcnd_nyc/2025
skip_quality=false
```

Se o Data Quality retornar `FAIL`, a Silver NOAA nao e publicada e o job deve
falhar de forma controlada.

### Gold Daily Weather Demand

Parametros principais no ADF:

```text
year
tlc_input
noaa_input
taxi_zone_lookup_input
output
mode
skip_count
dry_run
```

Exemplo:

```text
tlc_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
noaa_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd_nyc/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/gold/daily_weather_demand/2025
```

### Validate Gold Daily Weather Demand

Parametros principais no ADF:

```text
year
input
expected_days
min_days_with_demand
min_total_trips
allow_incomplete_weather
dry_run
```

Exemplo:

```text
year=2025
input=/Volumes/<catalog>/<schema>/<volume>/delta/gold/daily_weather_demand/2025
expected_days=365
min_days_with_demand=1
min_total_trips=1
allow_incomplete_weather=false
dry_run=false
```

Esse notebook valida a base diaria usada em EDA/ML. Se retornar `FAIL`, o
notebook falha e o ADF marca o pipeline como falho.

### Gold Star Schema

Parametros principais no ADF:

```text
year
tlc_input
noaa_input
taxi_zone_lookup_input
output
mode
skip_count
dry_run
```

Exemplo:

```text
tlc_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
noaa_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd_nyc/2025
taxi_zone_lookup_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/taxi_zone_lookup
output=/Volumes/<catalog>/<schema>/<volume>/delta/gold/star_schema/2025
```

### Validate Gold Star Schema

Parametros principais no ADF:

```text
year
input
expected_days
expected_locations
min_fact_rows
allow_incomplete_weather
dry_run
```

Exemplo:

```text
year=2025
input=/Volumes/<catalog>/<schema>/<volume>/delta/gold/star_schema/2025
expected_locations=265
min_fact_rows=1
allow_incomplete_weather=false
dry_run=false
```

Esse notebook deve ser a ultima etapa da Gold dimensional. Se a validacao retornar
`FAIL`, o notebook falha e o ADF marca o pipeline como falho.

## Uso Pelo ADF

O ADF deve chamar o notebook com Databricks Notebook Activity e passar os
parametros via `baseParameters`.

Exemplo NYC TLC:

```text
output=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/yellow/2025
```

Exemplo Taxi Zone Lookup:

```text
output=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/taxi_zone_lookup
```

Exemplo NOAA:

```text
/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd_nyc/2025
```

Tambem pode ser usado:

```text
dbfs:/mnt/raw/noaa/ghcnd_nyc/2025
```

O script converte `dbfs:/...` para `/dbfs/...` automaticamente.
