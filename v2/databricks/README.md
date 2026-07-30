# Databricks V2

Wrappers para executar a V2 no Azure Databricks.

Esses arquivos nao concentram regra de negocio. Eles apenas:

- recebem parametros do ADF ou Databricks Job;
- buscam secrets no Databricks quando necessario;
- chamam os scripts em `v2/pipelines`.

## Notebooks

```text
v2/databricks/notebooks/ingest_nyc_tlc.py
v2/databricks/notebooks/ingest_noaa_weather.py
v2/databricks/notebooks/bronze_nyc_tlc.py
v2/databricks/notebooks/bronze_noaa_weather.py
v2/databricks/notebooks/silver_nyc_tlc.py
v2/databricks/notebooks/silver_noaa_weather.py
v2/databricks/notebooks/gold_daily_weather_demand.py
v2/databricks/notebooks/gold_star_schema.py
```

## Ordem Do Pipeline

```text
1. ingest_nyc_tlc
2. ingest_noaa_weather
3. bronze_nyc_tlc
4. bronze_noaa_weather
5. silver_nyc_tlc
6. silver_noaa_weather
7. gold_daily_weather_demand
8. gold_star_schema
```

Os notebooks de Bronze, Silver e Gold reutilizam as funcoes PySpark em
`v2/pipelines`. Eles nao devem concentrar regra de negocio.

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

- receber ano, estacao e caminho raw;
- buscar token NOAA via `dbutils.secrets`;
- executar a ingestao NOAA com paginacao por `offset`;
- salvar paginas JSON no raw da cloud.

Parametros principais no ADF:

```text
year
stationid
output
secret_scope
secret_key
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
input=/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/noaa/ghcnd/2025
```

### Silver NYC TLC

Parametros principais no ADF:

```text
year
input
output
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
```

### Silver NOAA Weather

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
input=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/noaa/ghcnd/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd/2025
```

### Gold Daily Weather Demand

Parametros principais no ADF:

```text
year
tlc_input
noaa_input
output
mode
skip_count
dry_run
```

Exemplo:

```text
tlc_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
noaa_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/gold/daily_weather_demand/2025
```

### Gold Star Schema

Parametros principais no ADF:

```text
year
tlc_input
noaa_input
output
mode
skip_count
dry_run
```

Exemplo:

```text
tlc_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
noaa_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/gold/star_schema/2025
```

## Uso Pelo ADF

O ADF deve chamar o notebook com Databricks Notebook Activity e passar os
parametros via `baseParameters`.

Exemplo NYC TLC:

```text
output=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/yellow/2025
```

Exemplo NOAA:

```text
/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd/2025
```

Tambem pode ser usado:

```text
dbfs:/mnt/raw/noaa/ghcnd/2025
```

O script converte `dbfs:/...` para `/dbfs/...` automaticamente.
