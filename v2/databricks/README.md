# Databricks V2

Wrappers para executar a V2 no Azure Databricks.

Esses arquivos nao concentram regra de negocio. Eles apenas:

- recebem parametros do ADF ou Databricks Job;
- buscam secrets no Databricks;
- chamam os scripts em `v2/pipelines`.

## Notebooks

```text
v2/databricks/notebooks/ingest_noaa_weather.py
```

Responsabilidade:

- receber ano, estacao e caminho raw;
- buscar token NOAA via `dbutils.secrets`;
- executar a ingestao NOAA com paginacao por `offset`;
- salvar paginas JSON no raw da cloud.

## Uso Pelo ADF

O ADF deve chamar o notebook com Databricks Notebook Activity e passar os
parametros via `baseParameters`.

Parametros principais:

```text
year
stationid
output
secret_scope
secret_key
```

Exemplo de output:

```text
/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd/2025
```

Tambem pode ser usado:

```text
dbfs:/mnt/raw/noaa/ghcnd/2025
```

O script converte `dbfs:/...` para `/dbfs/...` automaticamente.
