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
```

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
